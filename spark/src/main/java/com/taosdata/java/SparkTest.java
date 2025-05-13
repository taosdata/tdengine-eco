package com.taosdata.java;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import java.util.ArrayList;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

public class SparkTest {
    // connect info
    static String url = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
    static String driver = "com.taosdata.jdbc.ws.WebSocketDriver";
    static String user = "root";
    static String password = "taosdata";
    static String timeout = "60"; // seconds

    static private String groupId = "group1";
    static private String clientId = "clinet1";

    // td dialect
    public static void registerDialect() {
        JdbcDialect tdDialect = new TDengineDialect();
        JdbcDialects.registerDialect(tdDialect);
    }

    // create spark
    public static SQLContext createSpark(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return new SQLContext(sc);
    }

    // write data
    public static void writeToTDengine(Connection connection, int childTb, int insertRows) {
        Random rand = new Random();
        long ts = 1700000000001L;

        // stmt write
        try {
            for (int i = 0; i < childTb; i++) {
                String sql = String.format("INSERT INTO test.d%d using test.meters tags(%d,'location%d') VALUES (?,?,?,?) ", i, i, i);
                System.out.printf("prepare sql:%s\n", sql);
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                for (int j = 0; j < insertRows; j++) {
                    float current = (float) (10 + i * 0.01);
                    float phase = (float) (1 + i * 0.0001);
                    int voltage = 100 + rand.nextInt(20);

                    preparedStatement.setTimestamp(1, new Timestamp(ts + j));
                    preparedStatement.setFloat(2, current);
                    preparedStatement.setInt(3, voltage);
                    preparedStatement.setFloat(4, phase);
                    // submit
                    preparedStatement.executeUpdate();
                    System.out.printf("stmt insert test.d%d j=%d %d,%f,%d,%f\n", i, j, ts + j, current, voltage, phase);
                }
                preparedStatement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // prepare data
    public static void prepareDemoData() {
        // insert
        int childTb = 2;
        int insertRows = 20;
        Connection connection = null;
        Statement statement = null;

        try {
            // create TDengine JDBC driver
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();

            // sqls
            String[] sqls = {
                    "DROP TOPIC IF EXISTS topic_meters",
                    "DROP DATABASE IF EXISTS test",
                    "CREATE DATABASE test",
                    "CREATE TABLE test.meters(ts timestamp, current float, voltage int , phase float) tags(groupid int, location varchar(24))",
                    "CREATE TOPIC topic_meters as select * from test.meters;"
            };

            for (int i = 0; i < sqls.length; i++) {
                statement.executeUpdate(sqls[i]);
                System.out.printf("execute sql succ:%s\n", sqls[i]);
            }

            // write data
            writeToTDengine(connection, childTb, insertRows);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // free Statement
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            // close Connection
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // table
    public static void readFromTDengine(SQLContext sqlContext, String dbtable) {
        // query sql
        DataFrame df = sqlContext.read()
               .format("jdbc")
               .option("url", url)
               .option("driver", driver)
               .option("queryTimeout", timeout)
               .option("dbtable", dbtable)
               .load();

        String log = String.format("------------ show dbtable read:%s -----------", dbtable);
        System.out.println(log);

        // show schema
        df.printSchema();
        // show data
        df.show();
    }

    // create view
    public static void createSparkView(SQLContext sqlContext, String dbtable, String viewName) {
        // query sql from TDengine
        DataFrame df = sqlContext.read()
               .format("jdbc")
               .option("url", url)
               .option("driver", driver)
               .option("queryTimeout", timeout)
               .option("dbtable", dbtable)
               .load();

        // create view with spark
        df.registerTempTable(viewName);
    }

    // analysis data with spark sql
    public static void analysisDataWithSpark(SQLContext sqlContext) {
        createSparkView(sqlContext, "test.meters", "sparkMeters");

        // execute Spark sql
     
        String sparkSql = "SELECT " +
                "groupid, avg(voltage) as voltage_avg" +
                "FROM sparkMeters group by groupid";
    
        System.out.println(sparkSql);
        DataFrame result = sqlContext.sql(sparkSql);
        result.show();
    }

    // -------------- subscribe ----------------

    //
    // class ResultBean
    //
    public static class ResultBean {
        private Timestamp ts;
        private float     current;
        private int       voltage;
        private float     phase;
        private int       groupid;
        private String    location;

        public Timestamp getTs()             { return ts; }
        public float     getCurrent()        { return current;}
        public int       getVoltage()        { return voltage;}
        public float     getPhase()          { return phase;}
        public int       getGroupid()        { return groupid;}
        public String    getLocation()       { return location;}

        public void      setTs(Timestamp ts)          { this.ts       = ts;}
        public void      setCurrent(float current)    { this.current  = current;}
        public void      setVoltage(int voltage)      { this.voltage  = voltage;}
        public void      setPhase(float phase)        { this.phase    = phase;}
        public void      setGroupid(int groupid)      { this.groupid  = groupid;}
        public void      setLocation(String location) { this.location = location;}
    }

    public static class ResultDeserializer extends ReferenceDeserializer<ResultBean> {}
    
    // getSonsumer
    public static TaosConsumer<ResultBean> getConsumer() throws Exception {
        // property
        String cls        = "com.taosdata.java.SparkTest$ResultDeserializer";
        Properties config = new Properties();
        config.setProperty("td.connect.type",             "ws");
        config.setProperty("bootstrap.servers",           "localhost:6041");
        config.setProperty("auto.offset.reset",           "earliest");
        config.setProperty("msg.with.table.name",         "true");
        config.setProperty("enable.auto.commit",          "true");
        config.setProperty("auto.commit.interval.ms",     "1000");
        config.setProperty("group.id",                    "group1");
        config.setProperty("client.id",                   "clinet1");
        config.setProperty("td.connect.user",             "root");
        config.setProperty("td.connect.pass",             "taosdata");
        config.setProperty("value.deserializer",          cls);
        config.setProperty("value.deserializer.encoding", "UTF-8");

        try {
            // new consumer
            TaosConsumer<ResultBean> consumer= new TaosConsumer<>(config);
            System.out.printf("Create consumer successfully, host: %s, groupId: %s, clientId: %s%n",
                    config.getProperty("bootstrap.servers"),
                    config.getProperty("group.id"),
                    config.getProperty("client.id"));
            return consumer;
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create websocket consumer, " + 
                    "host: %s, groupId: %s, clientId: %s, ErrMessage: %s%n",
                    config.getProperty("bootstrap.servers"),
                    config.getProperty("group.id"),
                    config.getProperty("client.id"),
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }

    public static StructType generateSchema() {
        // schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("ts", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("current", DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField("voltage", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("phase", DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField("groupid", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }    

    public static void pollExample(SQLContext sqlContext, TaosConsumer<ResultBean> consumer) throws SQLException, JsonProcessingException {
        List<String> topics = Collections.singletonList("topic_meters");
        List<Row> data = new ArrayList<>();

        //
        // obtain data
        //
        try {
            // subscribe  topics
            consumer.subscribe(topics);
            System.out.println("Subscribe topics successfully.");
            for (int i = 0; i < 100; i++) {
                // poll data
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // Add your data processing logic here
                    // System.out.println("data: " + JsonUtil.getObjectMapper().writeValueAsString(bean));

                    // covert bean to row
                    data.add(RowFactory.create(
                        bean.getTs(),
                        bean.getCurrent(),
                        bean.getVoltage(),
                        bean.getPhase(),
                        bean.getGroupid(),
                        bean.getLocation()
                    ));
                    
                }
            }

        } catch (Exception ex) {
            // catch except
            System.out.printf("Failed to poll data, topic: %s, %sErrMessage: %s%n",
                    topics.get(0),
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            ex.printStackTrace();
        }

        //
        // put to spark dataframe and show
        //
        StructType schema = generateSchema();
        DataFrame df      = sqlContext.createDataFrame(data, schema);

        // show
        System.out.println("------------- below is subscribe data --------------");
        df.show();
    }

    // subscribe
    public static void subscribeFromTDengine(SQLContext sqlContext) {
        try {
            TaosConsumer<ResultBean> consumer = getConsumer();

            pollExample(sqlContext, consumer);
            System.out.println("pollExample executed successfully.");
            consumer.unsubscribe();
            consumer.close();
        } catch (SQLException ex) {
            System.out.println("Failed to poll data from topic_meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            return;
        } catch (Exception ex) {
            System.out.println("Failed to poll data from topic_meters, ErrMessage: " + ex.getMessage());
            return;
        }
    }

    // main
    public static void main(String[] args) {
        // register dialect
        registerDialect();

        // create spark
        SQLContext sqlContext = createSpark("appSparkTest");

        // prepare demo data
        prepareDemoData();

        // read table
        String dbtable = "test.meters";
        readFromTDengine(sqlContext, dbtable);

        // spark sql analysis data
        analysisDataWithSpark(sqlContext);

        // subscribe
        subscribeFromTDengine(sqlContext);

        // stop
        sqlContext.sparkContext().stop();
    }
}
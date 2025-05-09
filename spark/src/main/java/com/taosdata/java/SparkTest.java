package com.taosdata.java;


import java.sql.PreparedStatement;
import java.sql.Timestamp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.taosdata.java.SparkTest.ResultBean;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;


import java.time.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;


import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.java_websocket.framing.DataFrame;




public class SparkTest {	
	// connect info
	static String url      = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
	static String driver   = "com.taosdata.jdbc.ws.WebSocketDriver";
	static String user     = "root";
	static String password = "taosdata";
	static int    timeout  = 60; // seconds

    static private String groupId = "group1";
    static private String clientId = "clinet1";    

	// td dialect
	public static void registerDialect() {
		JdbcDialect tdDialect = new TDengineDialect();
		JdbcDialects.registerDialect(tdDialect);
	}

	// create spark
	public static SparkSession createSpark(String appName) {
        return SparkSession.builder()
		.appName(appName)
		.master("local[*]")
		.getOrCreate();
	}

    // write data
    public static void writeToTDengine(SparkSession spark, int childTb, int insertRows) {
        // 创建数据结构
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("ts", DataTypes.TimestampType, true));
        fields.add(DataTypes.createStructField("current", DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField("voltage", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("phase", DataTypes.FloatType, true));
        fields.add(DataTypes.createStructField("groupid", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        
        // 准备数据
        List<Row> data = new ArrayList<>();
        Random rand = new Random();
        long ts = 1700000000001L;
        
        for (int i = 0; i < childTb; i++) {
            for (int j = 0; j < insertRows; j++) {
                float current = (float) (10 + i * 0.01);
                float phase = (float) (1 + i * 0.0001);
                int voltage = 100 + rand.nextInt(20);
                
                data.add(RowFactory.create(
                    new Timestamp(ts + j),
                    current,
                    voltage,
                    phase,
                    i,
                    "location" + i
                ));
            }
        }
        
        // 创建 DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.show();
        
        // 写入 TDengine
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        connectionProperties.put("driver", driver);
        connectionProperties.put("queryTimeout", "23");
        
        df.write()
          .mode(SaveMode.Append)
          .jdbc(url, "test.ntb", connectionProperties);
          
        System.out.println("DataFrame 已成功写入 TDengine");
    }


    // prepare data
    public static void prepareDemoData(SparkSession spark) {
        // insert
        Connection connection = null;
        Statement statement   = null;


        try {
            // create TDengine JDBC driver
            connection = DriverManager.getConnection(url);
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
            int childTb    = 1;
            int insertRows = 21;    
            writeToTDengine(spark, childTb, insertRows);  
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
	public static void readFromTDengine(SparkSession spark, String dbtable) {
        // create reader
        DataFrameReader reader = spark.read()
				.format("jdbc") 
				.option("url", url)
				.option("driver", driver)
				.option("queryTimeout", timeout);

        // map table
        Dataset<Row> df = reader.option("dbtable", dbtable).load();

		String log = String.format("------------ show dbtable read:%s -----------", dbtable);
		System.out.println(log);

		// show schema
        df.printSchema();
		// show data
        df.show(Integer.MAX_VALUE, 40, false);

        System.out.println("test read successfully!");
	}

	// create view
	public static void createSparkView(SparkSession spark, String sql, String viewName) {
        // query sql from TDengine
		Dataset<Row> df = spark.read()
				.format("jdbc") 
				.option("url", url)
				.option("driver", driver)
				.option("queryTimeout", timeout)
				.option("query", sql)
				.load();

        // create view with spark
        df.createOrReplaceTempView(viewName);

        return ;
	}

    // analysis data with spark sql
    public static void analysisDataWithSpark(SparkSession spark) {
		String sql = "select tbname,* from test.meters where tbname='d0'";
		createSparkView(spark, sql, "sparkMeters");
        
        // execute Spark sql
        String sparkSql = "SELECT " +
                "tbname, ts, voltage, " +
                "(LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, " +
                "CONCAT(ROUND(((voltage - voltage_last_week) / voltage_last_week * 100), 1),'%') AS weekly_growth_rate " +
                "FROM sparkMeters";
        
        System.out.println(sparkSql);
        Dataset<Row> result = spark.sql(sparkSql);
        result.show(Integer.MAX_VALUE, 40, false);
    }

    // -------------- subscribe ----------------

    public static class ResultDeserializer extends ReferenceDeserializer<ResultBean> {

    }

    // use this class to define the data structure of the result record
    public static class ResultBean {
        private Timestamp ts;
        private double current;
        private int voltage;
        private double phase;
        private int groupid;
        private String location;

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        public double getCurrent() {
            return current;
        }

        public void setCurrent(double current) {
            this.current = current;
        }

        public int getVoltage() {
            return voltage;
        }

        public void setVoltage(int voltage) {
            this.voltage = voltage;
        }

        public double getPhase() {
            return phase;
        }

        public void setPhase(double phase) {
            this.phase = phase;
        }

        public int getGroupid() {
            return groupid;
        }

        public void setGroupid(int groupid) {
            this.groupid = groupid;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }
    
    public static TaosConsumer<ResultBean> getConsumer() throws Exception {
        Properties config = new Properties();
        config.setProperty("td.connect.type", "ws");
        config.setProperty("bootstrap.servers", "localhost:6041");
        config.setProperty("auto.offset.reset", "earliest");
        config.setProperty("msg.with.table.name", "true");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("group.id", "group1");
        config.setProperty("client.id", "clinet1");
        config.setProperty("td.connect.user", "root");
        config.setProperty("td.connect.pass", "taosdata");
        config.setProperty("value.deserializer", "com.taosdata.java.SparkTest$ResultDeserializer");
        config.setProperty("value.deserializer.encoding", "UTF-8");

        try {
            TaosConsumer<ResultBean> consumer= new TaosConsumer<>(config);
            System.out.printf("Create consumer successfully, host: %s, groupId: %s, clientId: %s%n",
                    config.getProperty("bootstrap.servers"),
                    config.getProperty("group.id"),
                    config.getProperty("client.id"));
            return consumer;
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create websocket consumer, host: %s, groupId: %s, clientId: %s, %sErrMessage: %s%n",
                    config.getProperty("bootstrap.servers"),
                    config.getProperty("group.id"),
                    config.getProperty("client.id"),
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }

    public static void pollExample(TaosConsumer<ResultBean> consumer) throws SQLException, JsonProcessingException {
        List<String> topics = Collections.singletonList("topic_meters");
        try {
            // subscribe to the topics
            consumer.subscribe(topics);
            System.out.println("Subscribe topics successfully.");
            for (int i = 0; i < 100; i++) {
                // poll data
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // Add your data processing logic here
                    System.out.println("data: " + JsonUtil.getObjectMapper().writeValueAsString(bean));
                }
            }
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to poll data, topic: %s, groupId: %s, clientId: %s, %sErrMessage: %s%n",
                    topics.get(0),
                    groupId,
                    clientId,
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }    

    // subscribe
    public static void subscribeFromTDengine() {
        try {
            TaosConsumer<ResultBean> consumer = getConsumer();

            pollExample(consumer);
            System.out.println("test subscribe successfully!");
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
		// reister dialect
		registerDialect();

		// create spark
		SparkSession spark = createSpark("appSparkTest");

        // prepare demo data
        prepareDemoData(spark);

		// read table
		String dbtable = "test.meters";
		readFromTDengine(spark, dbtable);
        
		// spark sql analysis data
        analysisDataWithSpark(spark);

        // subscribe 
        subscribeFromTDengine();
        

        // stop
        spark.stop();
    }
}

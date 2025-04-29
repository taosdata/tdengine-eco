package com.taosdata.java;


import java.sql.PreparedStatement;
import java.security.Timestamp;

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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.java_websocket.framing.DataFrame;

public class SparkTest {	
	// connect info
	static String url      = "jdbc:TAOS-WS://localhost:6041/test?user=root&password=taosdata";
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

    // prepare data
    public static void prepareDemoData() {
        // insert
        int childTb    = 2;
        int insertRows = 20;
        Connection connection = null;
        Statement statement   = null;


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
            
            String sql;
            Random rand = new Random();
            long ts = 1700000000001L;
            // insert data
            for (int i = 0; i < childTb; i++ ) {
                sql = String.format("create table test.d%d using test.meters tags(%d, 'location%d')", i, i, i);
                statement.executeUpdate(sql);
                System.out.printf("execute sql succ:%s\n", sql);
                for (int j = 0; j < insertRows; j++) {
                    float current = (float)(10  + i * 0.01);
                    float phase   = (float)(1   + i * 0.0001);
                    int   voltage = 100 + rand.nextInt(20);
                    sql = String.format("insert into test.d%d values(%d, %f, %d, %f)", i, ts + j, current, voltage, phase);
                    statement.executeUpdate(sql);
                    System.out.printf("execute sql succ:%s\n", sql);
                }
            }
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
        // query sql
		Dataset<Row> df = spark.read()
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
            for (int i = 0; i < 50; i++) {
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
            System.out.println("pollExample executed successfully.");
            consumer.unsubscribe();

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
        prepareDemoData();

		// read table
		String dbtable = "test.meters";
		readFromTDengine(spark, dbtable);
        
		// execute TDengine sql
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


        subscribeFromTDengine();
        

        // stop
        spark.stop();
    }
}

package com.taosdata.java;

import org.apache.spark.sql.Dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;


public class SparkTest {	
	// connect info

    static private String groupId = "group1";
    static private String clientId = "clinet1";    

	// td dialect
	public static void registerDialect() {
		JdbcDialect tdDialect = new TDengineDialect();
		JdbcDialects.registerDialect(tdDialect);
	}

    // prepare env 
    public static void preareEnv() {
        // insert
        Connection connection = null;
        Statement statement   = null;
        try {
            // create TDengine JDBC driver
            String url = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
            connection = DriverManager.getConnection(url);
            statement  = connection.createStatement();
            
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

	// create view
	public static void createSparkView(SparkSession spark, String sql, String viewName) {
        // query sql from TDengine
        String url      = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
        String driver   = "com.taosdata.jdbc.ws.WebSocketDriver";
        int    timeout  = 60; // seconds
    
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
    public static void analysisDataWithSpark() {
        // create spark
        SparkSession spark = SparkSession.builder()
            .appName("appSparkTest")
            .master("local[*]")
            .getOrCreate();
        
        try {
            // create view
            String sql = "select tbname,* from test.meters where tbname='d0'";
            createSparkView(spark, sql, "sparkMeters");    

            String sparkSql = "SELECT " +
                    "tbname, ts, voltage, " +
                    "(LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, " +
                    "CONCAT(ROUND(((voltage - (LAG(voltage, 7) OVER (ORDER BY tbname))) / (LAG(voltage, 7) OVER (ORDER BY tbname)) * 100), 1),'%') AS weekly_growth_rate " +
                    "FROM sparkMeters";

            // execute Spark sql
            Dataset<Row> result = spark.sql(sparkSql);

            // show
            result.show(Integer.MAX_VALUE, 40, false);

            // out succ
            System.out.println("test analysis data successfully!");            

        } catch (Exception ex) {
            System.out.println("Failed to execute error Message: " + ex.getMessage());
            ex.printStackTrace();
            return;
        }

        // stop
        spark.stop();
    }

	// main
	public static void main(String[] args) {
		// reister dialect
		registerDialect();

        // prepare env
        preareEnv();

        // write data 
        DemoWrite.writeToTDengine(); 

		// read table
		DemoRead.readFromTDengine();

        // subscribe 
        DemoSubscribe.subscribeFromTDengine();
        
		// spark sql analysis data
        analysisDataWithSpark();
    }
}

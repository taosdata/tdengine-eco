package com.taosdata.java;


import java.sql.PreparedStatement;
import java.security.Timestamp;

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
        int childTb    = 1;
        int insertRows = 30;
        Connection connection = null;
        Statement statement   = null;


        try {
            // create TDengine JDBC driver
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
            
            // sqls
            String[] sqls = {
                "DROP DATABASE IF EXISTS test",
                "CREATE DATABASE test",
                "CREATE TABLE test.meters(ts timestamp, current float, voltage int , phase float) tags(groupid int, location varchar(24))"
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

	// sql
	public static Dataset<Row> executeSql(SparkSession spark, String sql, String viewName) {
        // query sql
		Dataset<Row> df = spark.read()
				.format("jdbc") 
				.option("url", url)
				.option("driver", driver)
				.option("queryTimeout", timeout)
				.option("query", sql)
				.load();

		System.out.println("------------ show sql query -----------");
		System.out.println(sql);

        // show schema
        df.printSchema();
        // show data
        df.show();

        // regitser view if need
        if (viewName != null) {
            df.createOrReplaceTempView(viewName);
        }

        return df;
	}

	// table
	public static void readTable(SparkSession spark, String dbtable) {
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
		readTable(spark, dbtable);
        
		// execute TDengine sql
		String sql = "select tbname,* from test.meters ";
		executeSql(spark, sql, "viewMeters");
        
        // execute Spark sql
        Dataset<Row> result = spark.sql("SELECT " +
                "tbname, ts, voltage, " +
                "(LAG(voltage, 7) OVER (ORDER BY tbname)) AS voltage_last_week, " +
                "CONCAT(ROUND(((voltage - voltage_last_week) / voltage_last_week * 100), 1),'%') AS weekly_growth_rate " +
                "FROM viewMeters");

        result.show(Integer.MAX_VALUE, 40, false);                

        // stop
        spark.stop();
    }
}

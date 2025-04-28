package com.taosdata.java;


import java.sql.PreparedStatement;
import java.security.Timestamp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

public class SparkTest {
    /* 
	public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
               .appName("SparkTDengineSQLExample")
               .master("local[*]")
               .getOrCreate();

        // TDengine 连接信息
        String jdbcUrl = "jdbc:TAOS-WS://localhost:6041/";
        String username = "root";
        String password = "taosdata";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             Statement statement = connection.createStatement()) {

            // 创建数据库
            String createDatabaseSQL = "CREATE DATABASE IF NOT EXISTS test_db";
            statement.executeUpdate(createDatabaseSQL);
            System.out.println("数据库创建成功");

            // 使用数据库
            String useDatabaseSQL = "USE test_db";
            statement.executeUpdate(useDatabaseSQL);
            System.out.println("已切换到 test_db 数据库");

            // 创建表
            String createTableSQL = "CREATE TABLE IF NOT EXISTS sensor_data (" +
                    "ts TIMESTAMP, " +
                    "sensor_id INT, " +
                    "temperature FLOAT, " +
                    "humidity FLOAT" +
                    ")";
            statement.executeUpdate(createTableSQL);
            System.out.println("表创建成功");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 停止 SparkSession
        spark.stop();
    }
    */


	/* 
    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
              .appName("SparkToTDengine")
              .master("local[*]")
              .getOrCreate();


        // 连接到 TDengine
        String jdbcUrl = "jdbc:TAOS-WS://localhost:6041/test";
        String username = "root";
        String password = "taosdata";
		//int ts = 1700000000;
        long currentTimeMillis = System.currentTimeMillis();
        // 创建 Timestamp 对象 

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            // 插入数据
            String insertQuery = "INSERT INTO ntb(ts, age) VALUES (?,?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                for (int i = 0 ; i < 10; i++) {
                    try {
						java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(currentTimeMillis + i);
                        preparedStatement.setTimestamp(1, currentTimestamp);
                        preparedStatement.setInt(2, i);
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 停止 SparkSession
        spark.stop();
    }
	*/
	
	
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
        int childTb    = 3;
        int insertRows = 10;
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
            long ts = 1700000000000L;
            // insert data
            for (int i = 0; i < childTb; i++ ) {
                sql = String.format("create table test.d%d using test.meters tags(%d, 'location%d')", i, i, i);
                statement.executeUpdate(sql);
                System.out.printf("execute sql succ:%s\n", sql);
                for (int j = 0; j < insertRows; j++) {
                    float current = (float)(10  + i * 0.01);
                    float phase   = (float)(1   + i * 0.0001);
                    int   voltage = 100 + i;
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
	public static void executeSql(SparkSession spark, String sql) {
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

		// sql
		String sql = "select * from test.meters limit 10";
		executeSql(spark, sql);

		// read table
		String dbtable = "test.meters";
		readTable(spark, dbtable);

        // analysis rate for weekly
 
        spark.stop();
    }
}

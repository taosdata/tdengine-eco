package com.taosdata.java;

import org.apache.spark.sql.Dataset;

import java.sql.SQLException;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DemoRead {	
	// read
	public static void readFromTDengine() {
        // create spark
        SparkSession spark = SparkSession.builder()
            .appName("appSparkTest")
            .master("local[*]")
            .getOrCreate();

        // connect
        String url      = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
        String driver   = "com.taosdata.jdbc.ws.WebSocketDriver";
        int    timeout  = 60; // seconds
        try {
            // query sql
            DataFrameReader reader = spark.read()
                    .format("jdbc") 
                    .option("url", url)
                    .option("driver", driver)
                    .option("queryTimeout", timeout);

            // map table
            String dbtable  = "test.meters";
            Dataset<Row> df = reader.option("dbtable", dbtable).load();
            String log      = String.format("------------ show dbtable read:%s -----------", dbtable);
            System.out.println(log);            
        } catch (SQLException ex) {
            System.out.println("Failed to read SQL error Message: " + ex.getMessage());
            ex.printStackTrace();
            return;
        } catch (Exception ex) {
            System.out.println("Failed to read error Message: " + ex.getMessage());
            ex.printStackTrace();
            return;
        } 

		// show schema
        df.printSchema();

		// show data
        df.show(Integer.MAX_VALUE, 40, false);

        // out succ log
        System.out.println("test read successfully!");
	}
}

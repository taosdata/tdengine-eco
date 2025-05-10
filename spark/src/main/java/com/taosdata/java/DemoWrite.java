package com.taosdata.java;

import java.util.Random;

import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


public class DemoWrite {	
    // write
    public static void writeToTDengine() {
        // create spark
        SparkSession spark = SparkSession.builder()
            .appName("appSparkTest")
            .master("local[*]")
            .getOrCreate();
    
        // stmt write
        Random rand       = new Random();
        long   ts         = 1700000000001L;
        int    childTb    = 1;
        int    insertRows = 21;  
        try {
            // connect
            String     url        = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
            Connection connection = DriverManager.getConnection(url);

            // write
            for (int i = 0; i < childTb; i++ ) {
                String sql = String.format("INSERT INTO test.d%d using test.meters tags(%d,'location%d') VALUES (?,?,?,?) ", i, i, i);
                System.out.printf("prepare sql:%s\n", sql);
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                for (int j = 0; j < insertRows; j++) {
                    float current = (float)(10  + rand.nextInt(100) * 0.01);
                    float phase   = (float)(1   + rand.nextInt(100) * 0.0001);
                    int   voltage = (int)  (210 + rand.nextInt(20));

                    preparedStatement.setTimestamp(1, new Timestamp(ts + j));
                    preparedStatement.setFloat    (2, current);
                    preparedStatement.setInt      (3, voltage);
                    preparedStatement.setFloat    (4, phase);
                    // add batch
                    preparedStatement.addBatch();
                    
                    System.out.printf("stmt insert test.d%d j=%d %d,%f,%d,%f\n", i, j, ts + j, current, voltage, phase);
                }
                // submit
                preparedStatement.executeUpdate();

                // close statement
                preparedStatement.close();
            }

            // close
            connection.close();

            // out succ
            System.out.println("test write successfully!");
        } catch (SQLException ex) {
            System.out.println("Failed to write data SQL error Message: " + ex.getMessage());
            ex.printStackTrace();
        } catch (Exception ex) {
            System.out.println("Failed to write data error Message: " + ex.getMessage());
            ex.printStackTrace();
        }

        // stop
        spark.stop();
    }
}
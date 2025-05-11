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
        Random rand = new Random();
        long   ts   = 1700000000001L;
        try {
            // connect
            String     url        = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
            Connection connection = DriverManager.getConnection(url);

            // write
            int childTb    = 1;
            int insertRows = 21;
            for (int i = 0; i < childTb; i++ ) {
                String sql = "INSERT INTO test.meters(tbname, groupid, location, ts, current, voltage, phase) " +
                    "VALUES (?,?,?,?,?,?,?)";
                System.out.printf("prepare sql:%s\n", sql);
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                for (int j = 0; j < insertRows; j++) {
                    float current = (float)(10  + rand.nextInt(100) * 0.01);
                    float phase   = (float)(1   + rand.nextInt(100) * 0.0001);
                    int   voltage = (int)  (210 + rand.nextInt(20));

                    preparedStatement.setString   (1, String.format("d%d", i));        // tbname
                    preparedStatement.setInt      (2, i);                              // groupid
                    preparedStatement.setString   (3, String.format("location%d", i)); // location

                    preparedStatement.setTimestamp(4, new Timestamp(ts + j));
                    preparedStatement.setFloat    (5, current);
                    preparedStatement.setInt      (6, voltage);
                    preparedStatement.setFloat    (7, phase);
                    // add batch
                    preparedStatement.addBatch();

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
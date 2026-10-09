package com.taosdata.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialects;

import com.taosdata.spark.TDengineDialect;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

/**
 * Integration test for the Spark demo with the official TDengine Spark dialect
 * (com.taosdata.spark:tdengine-spark-dialect).
 *
 * Runs Spark in local mode against the local TDengine server
 * (jdbc:TAOS-WS://localhost:6041), mirroring the write/read flow of the demo.
 * The test is skipped automatically when the server is not reachable.
 */
public class SparkIntegrationTest {

    private static final String URL    = System.getProperty("tdengine.ws.url",
            "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata&varcharAsString=true");
    private static final String DRIVER = "com.taosdata.jdbc.ws.WebSocketDriver";

    private static final int  CHILD_TABLES   = 2;
    private static final int  ROWS_PER_TABLE = 21;
    private static final long TOTAL_ROWS     = (long) CHILD_TABLES * ROWS_PER_TABLE;

    // dedicated database for this test run, unique per JVM launch so the test
    // never touches a pre-existing database on a reachable server
    private static final String DB_NAME = "spark_it_" + System.currentTimeMillis();

    private static SparkSession spark;
    private static boolean databaseCreated;

    @BeforeClass
    public static void setUp() throws Exception {
        // skip the whole test class when TDengine is not reachable
        Connection connection;
        try {
            connection = DriverManager.getConnection(URL);
        } catch (SQLException e) {
            assumeNoException("TDengine is not reachable at localhost:6041, skip integration test", e);
            return;
        }

        // create a fresh database and super table for this run only
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE DATABASE " + DB_NAME);
            // mark as created right away so tearDown cleans up on later failures
            databaseCreated = true;
            statement.executeUpdate("CREATE TABLE " + DB_NAME + ".meters(ts timestamp, current float, voltage int, phase float) " +
                    "tags(groupid int, location varchar(24))");
        }

        // write data via parameter binding, same as DemoWrite
        String sql = "INSERT INTO " + DB_NAME + ".meters(tbname, groupid, location, ts, current, voltage, phase) " +
                "VALUES (?,?,?,?,?,?,?)";
        long ts = 1700000000001L;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < CHILD_TABLES; i++) {
                for (int j = 0; j < ROWS_PER_TABLE; j++) {
                    ps.setString   (1, String.format("d%d", i));        // tbname
                    ps.setInt      (2, i);                              // groupid
                    ps.setString   (3, String.format("location%d", i)); // location
                    ps.setTimestamp(4, new Timestamp(ts + j));
                    ps.setFloat    (5, 10.0f + j * 0.01f);              // current
                    ps.setInt      (6, 210 + j % 20);                   // voltage
                    ps.setFloat    (7, 1.0f + j * 0.0001f);             // phase
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
        connection.close();

        // register the official TDengine dialect and start a local Spark session
        JdbcDialects.registerDialect(new TDengineDialect());
        spark = SparkSession.builder()
                .appName("sparkIntegrationTest")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (spark != null) {
            spark.stop();
        }
        // only drop the database when this run actually created it; in particular
        // do not open a new connection when the class was skipped
        if (!databaseCreated) {
            return;
        }
        try (Connection connection = DriverManager.getConnection(URL);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP DATABASE IF EXISTS " + DB_NAME);
        }
    }

    @Test
    public void dialectIsRegisteredForTaosWsUrl() {
        assertTrue(new TDengineDialect().canHandle(URL));
        assertTrue(JdbcDialects.get(URL) instanceof TDengineDialect);
    }

    @Test
    public void readSuperTableWithDialect() {
        // read via table mapping, same as DemoRead
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", URL)
                .option("driver", DRIVER)
                .option("queryTimeout", "60")
                .option("dbtable", DB_NAME + ".meters")
                .load();

        assertEquals(TOTAL_ROWS, df.count());
        // tag columns of the super table are visible and filterable
        assertEquals(ROWS_PER_TABLE, df.filter("groupid = 1").count());
    }
}

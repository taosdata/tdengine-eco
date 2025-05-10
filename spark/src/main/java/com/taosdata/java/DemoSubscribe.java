package com.taosdata.java;

import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.sql.Timestamp;
import java.sql.SQLException;

import java.util.ArrayList;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class DemoSubscribe {
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
        config.setProperty("value.deserializer",          "com.taosdata.java.DemoSubscribe$ResultDeserializer");
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

    // pollExample
    public static void pollExample(SparkSession spark, TaosConsumer<ResultBean> consumer) throws SQLException, JsonProcessingException {
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
        // put spark dataframe and show
        //
        StructType schema = generateSchema();
        Dataset<Row> df   = spark.createDataFrame(data, schema);

        // show
        System.out.println("----------------- below is subscribe data ----------------");
        df.show(Integer.MAX_VALUE, 40, false);
    }

    // subscribe
    public static void subscribeFromTDengine() {
        // create spark
        SparkSession spark = SparkSession.builder()
            .appName("appSparkTest")
            .master("local[*]")
            .getOrCreate();
            
            
        try {
            // consumer
            TaosConsumer<ResultBean> consumer = getConsumer();

            // poll
            pollExample(spark, consumer);

            // out succ
            System.out.println("test subscribe successfully!");

            // close
            consumer.unsubscribe();
            consumer.close();

        } catch (SQLException ex) {
            System.out.println("Failed to poll data from topic_meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
        } catch (Exception ex) {
            System.out.println("Failed to poll data from topic_meters, ErrMessage: " + ex.getMessage());
        }

        // stop
        spark.stop();
    }
}

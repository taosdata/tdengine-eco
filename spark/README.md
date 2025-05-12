# What is TD-Spark
Demo for Spark connect TDengine data source, supported reading/writing/subscribe function.

# Building 

## Install build dependencies

To install openjdk-8 and maven:
```
sudo apt-get install -y openjdk-8-jdk maven 
```

To install Spark:
```
wget https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar zxf spark-3.5.5-bin-hadoop3.tgz -C /usr/local/
export SPARK_HOME=/usr/local/spark-3.5.5-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

## Build

```
mvn clean package
```

# Run

* run the job
```
spark-submit --master local --name testSpark --class com.taosdata.java.SparkTest /testSpark-2.0-dist.jar
```

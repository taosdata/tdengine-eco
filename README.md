# What is TD-Flink
a java project,read from TDengine and write to TDengine on Spark.
# Building 
## Install build dependencies
To install openjdk-8 and maven:
```
sudo apt-get install -y openjdk-8-jdk maven 
```
To install Spark:
```
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
tar zxf spark-3.2.1-bin-hadoop3.2.tgz -C /usr/local
```
## Build
```
mvn clean package
```
# Run

* run the job
```
spark-submit --master local --name TDenginetest --class com.taosdata.java.SparkTest /testSpark-2.0-dist.jar
```

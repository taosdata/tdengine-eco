# What is TD-Spark
Demo for Spark connect TDengine data source, supported reading/writing/subscribe function.

# Building 

## Install build dependencies

To install openjdk-8 and maven:
```
sudo apt-get install -y openjdk-8-jdk maven 
```

To install Spark:
``` bash
wget https://archive.apache.org/dist/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
tar zxf spark-1.6.0-bin-hadoop2.6.tgz -C /usr/local/
export SPARK_HOME=/usr/local/spark-1.6.0-bin-hadoop2.6
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

## Build

``` bash
mvn clean package
```

# Run

run the job
``` bash
spark-submit \
  --driver-java-options "--add-opens java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
  --master local \
  --name TDenginetest \
  --class com.taosdata.java.SparkTest \
  target/testSpark-2.0.jar
```

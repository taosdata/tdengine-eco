# What is TD-Spark
Demo for Spark connect TDengine data source, supported reading/writing/subscribe function.

The demo registers the official TDengine Spark JDBC dialect ([tdengine-spark-dialect](https://github.com/taosdata/tdengine-spark-dialect)), which is declared in `pom.xml` and pulled from Maven Central at build time.

# Building 

## Install build dependencies

To install openjdk-8 and maven:
```
sudo apt-get install -y openjdk-8-jdk maven 
```

To install Spark (3.3.2 with Scala 2.13 — the runtime combination required by [tdengine-spark-dialect](https://github.com/taosdata/tdengine-spark-dialect) 1.0.0, matching the `spark-sql_2.13` 3.3.2 dependency declared in `pom.xml`):
```
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3-scala2.13.tgz
tar zxf spark-3.3.2-bin-hadoop3-scala2.13.tgz -C /usr/local/
export SPARK_HOME=/usr/local/spark-3.3.2-bin-hadoop3-scala2.13
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

## Build

```
mvn clean package
```

# Run

* run the job
```
spark-submit --master local --name testSpark \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --class com.taosdata.java.SparkTest /testSpark-2.0-dist.jar
```

Note: `taos-jdbcdriver` 3.9.2 requires Netty 4.2 while Spark distributions ship Netty 4.1, so `userClassPathFirst` is needed to let the Netty version bundled in the demo jar take precedence.

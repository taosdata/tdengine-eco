mvn clean package
spark-submit \
  --driver-java-options "--add-opens java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
  --master local \
  --name TDenginetest \
  --jars /root/taos-connector-jdbc/target/taos-jdbcdriver-3.7.0-SNAPSHOT.jar \
  --class com.taosdata.java.SparkTest \
  target/testSpark-2.0.jar

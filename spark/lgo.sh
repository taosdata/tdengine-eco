mvn clean package
spark-submit \
  --driver-java-options "--add-opens java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens java.base/java.lang=ALL-UNNAMED" \
  --master local \
  --name TDenginetest \
  --class com.taosdata.java.SparkTest \
  target/testSpark-2.0.jar

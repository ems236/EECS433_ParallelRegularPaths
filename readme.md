DEBUG:
$SPARK_HOME/bin/spark-submit   --class "SimpleApp"   --master local[1]  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 target/scala-2.11/regular_path_queries_2.11-0.1.jar

RUN:
$SPARK_HOME/bin/spark-submit   --class "SimpleApp"   --master local[1]  target/scala-2.11/regular_path_queries_2.11-0.1.jar

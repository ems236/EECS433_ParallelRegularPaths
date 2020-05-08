Ellis Saupe ems236

Main class for testing reachability queries is RandomQueryTest

This also includes an implementation for pattern query processing on a Pregel model.  It appears to work on the demo example in SimpleApp.main.
I got it working too late to do large scale tests or put it in the paper, but it is pretty much written.  



Run commands:

DEBUGER:
$SPARK_HOME/bin/spark-submit   --class "RandomQueryTest"   --master local[4]  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 target/scala-2.11/regular_path_queries_2.11-0.1.jar

RUN without debugger:
$SPARK_HOME/bin/spark-submit   --class "RandomQueryTest"   --master local[4]  target/scala-2.11/regular_path_queries_2.11-0.1.jar

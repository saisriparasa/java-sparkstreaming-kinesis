java-sparkstreaming-kinesis
===========================

This project is to run a Spark Streaming application in Java

mvn clean compile assembly:assembly >> to Build the jar with deps

spark-submit --class "com.java_mvn_sparkstreaming.SparkStreaming.JavaSparkStreaming" SparkStreaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar >> To run on the Spark Master

TODO:

Remove the streaming context stop, and let the job run
Seperate the Strings on Tab and use the 2nd part to map & reduce >> should be a different spark project
Integrate the above spark project into this one

<h1>Apache Spark and Spark Streaming example</h1>

I have used hortonworks sandbox and installed Apache Spark and Spark Streaming.

<strong>Running the simple app on Spark</strong>

cd /home/spark/spark-1.0.1 (Spark Installation Directory)

./bin/spark-submit --class sagar.spark.example.SimpleApp examples/sagar-spark-0.0.1-SNAPSHOT.jar

Another example from Spark Streaming is to read the file from flume agent using avro sink.

<strong>To Start the Avro Sink</strong>
flume-ng agent -c /etc/flume/conf -f /etc/flume/conf/flumeavro.conf -n sandbox

<strong>start Spark Streaming example</strong>
./bin/spark-submit examples/sagar-spark-0.0.1-SNAPSHOT-jar-with-dependencies.jar --class sagar.spark.streaming.example.JavaFlumeEventCount 127.0.0.1 41414



<strong>flumeavro.conf is checked in resource folder</strong>
<strong>createRuntime.sh to read omniturelog and write to another log file to simulate real time streaming</strong>


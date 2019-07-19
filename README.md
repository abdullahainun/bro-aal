source code ini digunakan untuk menjalankan modelling data, stream classification pada platform big data(kafka, apache spark, mongodb)

# requirement
- apache spark 2.3.2
- kafka
- mongodb
- scala

# how to run decision tree training
```
$ sbt assembly
$ spark-submit --master spark://192.168.58.1:7077 --executor-memory 2G --total-executor-cores 2 --class com.aal.spark.jobs.SimpleDT target/scala-2.11/bro-dns-streaming-assembly-0.1.jar
```
# how to stream classification realtime traffic
```
$ sbt assembly
$ spark-submit --master spark://192.168.58.1:7077 --executor-memory 2G --total-executor-cores 2 --class com.aal.spark.jobs.StreamClassification target/scala-2.11/bro-dns-streaming-assembly-0.1.jar

```

# Distributed-CoreNLP
This infrastructure, built on Stanford CoreNLP, MapReduce and Spark, aims at processing documents annotations at large scale.

### Build with Maven
1. Make sure you have Maven installed, details here: https://maven.apache.org/
2. If you run this command in the Spark-CoreNLP directory: `mvn clean package` , it should build this jar file: `target/project-1.0.jar`

### Run with MapReduce
3.2. Now, run a job using the following command: 
```bash
hadoop jar target/project-1.0.jar ca.uwaterloo.cs651.MapReduce.CoreNLPMapReduce -input ${input path} -output ${output path} -functionality ${func1,func2,func3,...}
```

### Run with Spark
3.1. Now, run a job using the following command: 
```bash
spark-submit --class ca.uwaterloo.cs651.project.CoreNLP --num-executors ${num of mappers} --executor-cores ${num of mappers} --conf spark.executor.heartbeatInterval=10s --conf spark.network.timeout=20s --driver-memory 6G --executor-memory 20G target/project-1.0.jar -input ${input path} -output ${output path} -mappers $mappers -functionality ${func1,func2,func3,...} 
```

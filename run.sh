# mvn clean package
# spark-submit --class ca.uwaterloo.cs651.project.SimpleNLP --driver-memory 4G --executor-memory 4G target/project-1.0.jar -input simpledata -output output -functionality tokenize,pos,lemma,ner,parse,natlog,openie,ssplit
# hdfs dfs -mkdir -p /user/k86huang/cs651
# hdfs dfs -put ./simpledata /user/k86huang/cs651
spark-submit --class ca.uwaterloo.cs651.project.CoreNLP --conf spark.network.timeout=600s --conf spark.executor.heartbeatInterval=20s --driver-memory 6G --executor-memory 24G target/project-1.0.jar -input simpledata -output output -functionality quote,natlog # natlog,sentiment # cleanxml,ssplit,coref


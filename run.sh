# mvn clean package
# spark-submit --class ca.uwaterloo.cs651.project.SimpleNLP --driver-memory 4G --executor-memory 4G target/project-1.0.jar -input simpledata -output output -functionality dcoref
spark-submit --class ca.uwaterloo.cs651.project.CoreNLP --driver-memory 4G --executor-memory 4G target/project-1.0.jar -input simpledata -output output -functionality dcoref

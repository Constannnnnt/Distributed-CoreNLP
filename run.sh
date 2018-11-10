mvn clean package
spark-submit --class ca.uwaterloo.cs651.project.CoreNLP target/project-1.0.jar -input sampledata
# spark-submit --class ca.uwaterloo.cs651.project.WordCount target/assignments-1.0.jar sampledata
# spark-submit --class ca.uwaterloo.cs651.project.SimpleSpark target/project-1.0.jar sampledata


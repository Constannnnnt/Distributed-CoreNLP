mvn clean package
# spark-submit --class ca.uwaterloo.cs651.project.CoreNLP target/project-1.0.jar -input sampledata
spark-submit --class ca.uwaterloo.cs651.project.CoreNLP --driver-memory 4G target/project-1.0.jar -input simpledata -output output -functionality ner

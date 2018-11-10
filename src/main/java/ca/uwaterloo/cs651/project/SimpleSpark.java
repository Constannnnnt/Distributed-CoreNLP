package ca.uwaterloo.cs651.project;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import edu.stanford.nlp.simple.*;

public final class SimpleSpark {

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: SimpleSpark <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession
      .builder()
      .appName("SimpleSpark")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<Sentence> sents = lines.map(line -> new Sentence(line));

    JavaRDD<String> words = sents
      .flatMap(sent -> sent.words().iterator());

    JavaRDD<String> parse = sents.map(sent -> sent.parse().toString());

    for (String w : words.collect()) {
      System.out.println(w);
    }
    
    for (String p : parse.collect()) {
      System.out.println(p);
    }

    spark.stop();
  }
}
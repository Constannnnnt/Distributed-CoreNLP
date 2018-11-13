package ca.uwaterloo.cs651.project;

import java.util.Set;
import java.util.TreeSet;
import java.util.Collection;
import java.util.Map;

import edu.stanford.nlp.simple.*;
import edu.stanford.nlp.naturalli.Polarity;
import edu.stanford.nlp.util.Quadruple;
import edu.stanford.nlp.coref.data.CorefChain;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;

import org.kohsuke.args4j.*;

public class SimpleNLP {
    private static final Logger LOG = Logger.getLogger(SimpleNLP.class);

    private static final Set availFuncSent = new TreeSet<String>();
    private static final Set availFuncDoc = new TreeSet<String>();

    private static void buildAvailFunc() {
        availFuncSent.add("tokenization");
        availFuncSent.add("pos");
        availFuncSent.add("lemma");
        availFuncSent.add("ner");
        availFuncSent.add("parse");
        availFuncSent.add("depparse");
        availFuncSent.add("natlog");
        availFuncSent.add("openie");

        availFuncDoc.add("ssplit");
        availFuncDoc.add("dcoref");
    }

    public static void main(String[] args) throws IllegalArgumentException { 
        final Args _args = new Args();
        CmdLineParser parser = new CmdLineParser(_args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return;
        }
     
        LOG.info("Tool: " + SimpleNLP.class.getSimpleName());
        LOG.info(" - input path: " + _args.input);
        LOG.info(" - output path: " + _args.output);
        LOG.info(" - functionalities: " + _args.functionality);

        String [] functionalities = _args.functionality.split(",");
        buildAvailFunc();
        for (String func : functionalities) {
            if (availFuncSent.contains(func) || availFuncDoc.contains(func))
                LOG.info("Functionality: " + func);
            else
                throw new IllegalArgumentException("No such functionality: " + func);
        }

        SparkSession spark = SparkSession
          .builder()
          .appName("SimpleNLP")
          .config("spark.hadoop.validateOutputSpecs", "false")
          .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(_args.input).javaRDD();
        JavaRDD<Sentence> sents = lines.map(line -> new Sentence(line));
        JavaRDD<Document> docs = lines.map(line -> new Document(line));

        for (String func : functionalities) switch (func) {
            case "tokenization": {
                JavaRDD<java.util.List<String>> words = sents
                    .map(sent -> sent.words());
                words.saveAsTextFile(_args.output+"/tokenization");
                break;
            }
            case "pos": {
                JavaRDD<java.util.List<String>> posTags = sents
                    .map(sent -> sent.posTags());
                posTags.saveAsTextFile(_args.output+"/pos");
                break;
            }
            case "lemma": {
                JavaRDD<java.util.List<String>> lemmas = sents
                    .map(sent -> sent.lemmas());
                lemmas.saveAsTextFile(_args.output+"/lemma");
                break;
            }
            case "ner": {
                JavaRDD<java.util.List<String>> nerTags = sents
                    .map(sent -> sent.nerTags());
                nerTags.saveAsTextFile(_args.output+"/ner");
                break;
            }
            case "parse": {
                JavaRDD<String> parse = sents
                    .map(sent -> sent.parse().toString());
                parse.saveAsTextFile(_args.output+"/parse");
                break;
            }
            case "depparse": {
                JavaRDD<String> parse = sents
                    .map(sent -> sent.governor(0).toString());
                parse.saveAsTextFile(_args.output+"/depparse");
                break;
            }
            case "natlog": {
                JavaRDD<java.util.List<Polarity>> natlogPolarities = sents
                    .map(sent -> sent.natlogPolarities());
                natlogPolarities.saveAsTextFile(_args.output+"/natlog");
                break;
            }
            case "openie": {
                JavaRDD<Collection<Quadruple<String,String,String,Double>>> openie = sents
                    .map(sent -> sent.openie());
                openie.saveAsTextFile(_args.output+"/openie");
                break;
            }

            case "ssplit": {
                JavaRDD<java.util.List<Sentence>> doc_sents = docs
                    .map(doc -> doc.sentences());
                doc_sents.saveAsTextFile(_args.output+"/ssplit");
                break;
            }
            case "dcoref": {
                JavaRDD<Map<Integer,CorefChain>> coref = docs
                    .map(doc -> doc.coref());
                coref.saveAsTextFile(_args.output+"/dcoref");
                break;
            }
        }

        spark.stop();
    }
}

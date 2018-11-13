package ca.uwaterloo.cs651.project;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.pipeline.CoreNLPProtos.Sentiment;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.simple.*;
import edu.stanford.nlp.util.Quadruple;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.*;
// import org.apache.spark.broadcast.Broadcast;
import org.kohsuke.args4j.*;

import java.util.Properties;
import java.util.stream.Collectors;

public class CoreNLP {
    private static final Logger LOG = Logger.getLogger(CoreNLP.class);
    
    public static void main(String[] args) { 
        final Args _args = new Args();
        CmdLineParser parser = new CmdLineParser(_args, ParserProperties.defaults().withUsageWidth(100));

    	try {
      	    parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return;
        }

        LOG.info("Tool: " + CoreNLP.class.getSimpleName());
        LOG.info("input path: " + _args.input);
        LOG.info("output path: " + _args.output);

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");
        props.setProperty("ner.useSUTime", "false");

        SparkSession spark = SparkSession
          .builder()
          .appName("CoreNLP")
          .config("spark.hadoop.validateOutputSpecs", "false")
          .getOrCreate();
	
	Broadcast<Properties> propsVar = spark.sparkContext().broadcast(props, scala.reflect.ClassTag$.MODULE$.apply(Properties.class));
        JavaRDD<String> lines = spark.read().textFile(_args.input).javaRDD();
        JavaRDD<CoreDocument> docs = lines.map(line -> new CoreDocument(line));

        docs.map(doc -> {
            StanfordCoreNLP pipeline = new StanfordCoreNLP(propsVar.getValue());
            pipeline.annotate(doc);
            return doc.tokens().stream().map(token -> "("+token.word()+","+token.ner()+")").collect(Collectors.joining(" "));
        })
        .saveAsTextFile(_args.output);

        spark.stop();

    }
}

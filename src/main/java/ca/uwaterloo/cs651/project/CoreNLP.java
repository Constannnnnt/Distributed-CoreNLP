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
import org.kohsuke.args4j.*;

import java.util.Properties;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class CoreNLP {
    private static final Logger LOG = Logger.getLogger(CoreNLP.class);

    private static final HashMap<String, String []> depChain = new HashMap();
    private static ArrayList<String> supportedFunc = new ArrayList<String>();
    private static ArrayList<String> funcToDo = new ArrayList<String>();

    private static void buildChain() {
        supportedFunc.add("tokenize");
        supportedFunc.add("cleanxml");
        supportedFunc.add("ssplit");
        supportedFunc.add("pos");
        supportedFunc.add("lemma");
        supportedFunc.add("ner");
        supportedFunc.add("regexner");
        supportedFunc.add("sentiment");
        supportedFunc.add("parse");
        supportedFunc.add("depparse");
        supportedFunc.add("dcoref");
        supportedFunc.add("coref");
        supportedFunc.add("relation");
        supportedFunc.add("natlog");
        supportedFunc.add("quote");

        String [] temp;
        temp = new String[]{};
        depChain.put("tokenize", temp);
        
        temp = new String[]{"tokenize"};
        depChain.put("cleanxml", temp);
        depChain.put("ssplit", temp);

        temp = new String[]{"tokenize", "ssplit"};
        depChain.put("pos", temp);
        depChain.put("parse", temp);

        temp = new String[]{"tokenize", "ssplit", "pos"};
        depChain.put("lemma", temp);
        depChain.put("regexner", temp);
        depChain.put("depparse", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma"};
        depChain.put("ner", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "parse"};
        depChain.put("sentiment", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
            "ner", "parse"};
        depChain.put("dcoref", temp);
        depChain.put("coref", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
            "ner", "depparse"};
        depChain.put("relation", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
            "parse"};
        depChain.put("natlog", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
            "ner", "depparse", "coref"};
        depChain.put("quote", temp);
    }

    private static String buildToDo(String [] functionalities) 
            throws IllegalArgumentException {
        buildChain();

        // change functionalites from String[] to Set
        HashSet<String> funcs = new HashSet<String>();
        for (String f : functionalities)
            funcs.add(f);

        String ans = "";
        for (String f : supportedFunc)
            if (funcs.contains(f)) {
                String [] deps = depChain.get(f);
                for (String d : deps)
                    if (funcToDo.indexOf(d)==-1) {
                        funcToDo.add(d);
                        ans += d+",";
                    }
                if (funcToDo.indexOf(f)==-1) {
                        funcToDo.add(f);
                        ans += f+",";
                    }
            }

        return ans.substring(0, ans.length()-1);
    }
    
    public static void main(String[] args) throws IllegalArgumentException { 
        final Args _args = new Args();
        CmdLineParser parser = new CmdLineParser(
            _args, ParserProperties.defaults().withUsageWidth(100));

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
        LOG.info(" - functionalities: " + _args.functionality);

        String [] functionalities = _args.functionality.split(",");
        String pipeline_input = buildToDo(functionalities);
        for (String f : funcToDo)
            System.out.println(f);
        System.out.println(pipeline_input);

        Properties props = new Properties();
        props.setProperty("annotators", pipeline_input);
        props.setProperty("ner.useSUTime", "false");

        SparkSession spark = SparkSession
          .builder()
          .appName("CoreNLP")
          .config("spark.hadoop.validateOutputSpecs", "false")
          .getOrCreate();
	
	    Broadcast<Properties> propsVar = spark.sparkContext().broadcast(
            props, scala.reflect.ClassTag$.MODULE$.apply(Properties.class));
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

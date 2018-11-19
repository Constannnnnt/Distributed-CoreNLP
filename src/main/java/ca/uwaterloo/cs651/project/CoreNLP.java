package ca.uwaterloo.cs651.project;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.pipeline.CoreNLPProtos.Sentiment;
import edu.stanford.nlp.simple.*;
import edu.stanford.nlp.util.Quadruple;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.coref.data.CorefChain.CorefMention;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.*;
import org.kohsuke.args4j.*;

import java.util.Properties;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class CoreNLP {
    private static final Logger LOG = Logger.getLogger(CoreNLP.class);

    private static final HashMap<String, String[]> depChain = new HashMap();
    private static ArrayList<String> supportedFunc = new ArrayList<String>();
    private static ArrayList<String> funcToDo = new ArrayList<String>();

    private static void buildChain() {
        supportedFunc.add("tokenize");//Assigned to Jayden
        supportedFunc.add("cleanxml");//Assigned to Constant
        supportedFunc.add("ssplit");//Assigned to Constant
        supportedFunc.add("pos");//Assigned to Rex
        supportedFunc.add("lemma");//Assigned to Rex
        supportedFunc.add("ner");//Assigned to Jayden
        supportedFunc.add("regexner");
        supportedFunc.add("sentiment");//Assigned to Constant
        supportedFunc.add("parse");//Assigned to Rex
        supportedFunc.add("depparse");//Assigned to Rex
        supportedFunc.add("dcoref");//Assigned to Constant
        supportedFunc.add("coref");//Assigned to Constant
        supportedFunc.add("relation");//Assigned to Rex
        supportedFunc.add("natlog");
        supportedFunc.add("quote");

        String[] temp;
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

        temp = new String[]{"tokenize", "ssplit", "parse"};
        depChain.put("sentiment", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
                "ner", "parse"};
        depChain.put("dcoref", temp);
        depChain.put("coref", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
                "ner", "parse", "depparse"};
        depChain.put("relation", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
                "parse"};
        depChain.put("natlog", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
                "ner", "depparse", "coref"};
        depChain.put("quote", temp);
    }

    private static String buildToDo(String[] functionalities)
            throws IllegalArgumentException {
        buildChain();

        // change functionalites from String[] to Set
        HashSet<String> funcs = new HashSet<String>();
        for (String f : functionalities)
            funcs.add(f);

        String ans = "";
        for (String f : supportedFunc)
            if (funcs.contains(f)) {
                String[] deps = depChain.get(f);
                for (String d : deps)
                    if (funcToDo.indexOf(d) == -1) {
                        funcToDo.add(d);
                        ans += d + ",";
                    }
                if (funcToDo.indexOf(f) == -1) {
                    funcToDo.add(f);
                    ans += f + ",";
                }
            }

        return ans.substring(0, ans.length() - 1);
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

        String[] functionalities = _args.functionality.split(",");
        String pipeline_input = buildToDo(functionalities);

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
        JavaPairRDD<String, Long> lines = spark.read().textFile(_args.input).javaRDD().zipWithIndex();

        lines.flatMapToPair(pair -> {
            Long index = pair._2();
            String line = pair._1();
            CoreDocument doc = new CoreDocument(line);
            Annotation anno = new Annotation(line);
            StanfordCoreNLP pipeline = new StanfordCoreNLP(propsVar.getValue());
            pipeline.annotate(doc);
            pipeline.annotate(anno);

            ArrayList<Tuple2<Tuple2<Long, String>, String>> mapResults = new ArrayList<>();
            for (String func : funcToDo) {
                if (func.equalsIgnoreCase("tokenize")) {
                    String ans = "";
                    for (CoreLabel word : anno.get(CoreAnnotations.TokensAnnotation.class))
                        ans += word.toString() + " ";
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            ans.substring(0, ans.length() - 1)));
                }
                else if (func.equalsIgnoreCase("cleanxml")) {
                    String ans = "";
                    for (CoreLabel word : anno.get(CoreAnnotations.TokensAnnotation.class)) 
                        ans += word.toString() + " "; 
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            ans.substring(0, ans.length() - 1)));
                }
                else if (func.equalsIgnoreCase("ssplit")) {
                    String ans = "";
                    for (CoreMap sentence: anno.get(CoreAnnotations.SentencesAnnotation.class))
			ans += sentence.toString() + " ";
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            ans.substring(0, ans.length() - 1)));
                } 
                else if (func.equalsIgnoreCase("pos")) {
                    String ans = doc.tokens().stream().map(token ->
                            "(" + token.word() + "," + token.get(CoreAnnotations.PartOfSpeechAnnotation.class) + ")")
                            .collect(Collectors.joining(" "));
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            ans));
                }
                else if (func.equalsIgnoreCase("ner")) {
                    String ans = doc.tokens().stream().map(token ->
                            "(" + token.word() + "," + token.ner() + ")").collect(Collectors.joining(" "));
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            ans));
                }
                else if (func.equalsIgnoreCase("sentiment")) {
                    int ans = -1;
                    for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                        Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                        ans = RNNCoreAnnotations.getPredictedClass(tree);
                    }
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            Integer.toString(ans) + "-" + line));
                } 
                else if (func.equalsIgnoreCase("coref")) {
                    String ans = "";
                    String tmpans = "";
                    Map<Integer, CorefChain> coref = anno.get(CorefCoreAnnotations.CorefChainAnnotation.class); 
                    for(Map.Entry<Integer, CorefChain> entry : coref.entrySet()) {
                        CorefChain cc = entry.getValue();
                            
                        //this is because it prints out a lot of self references which aren't that useful
                        if(cc.getMentionsInTextualOrder().size() <= 1) continue;

                        CorefMention cm = cc.getRepresentativeMention();
                        String clust = "";
    			List<CoreLabel> tks = anno.get(CoreAnnotations.SentencesAnnotation.class).get(cm.sentNum-1).get(CoreAnnotations.TokensAnnotation.class);
    			for(int i = cm.startIndex - 1; i < cm.endIndex - 1; i++) 
                            clust += tks.get(i).get(CoreAnnotations.TextAnnotation.class) + " ";
                        clust = clust.trim();
                        
                        tmpans += clust + ":";
                        for(CorefMention m : cc.getMentionsInTextualOrder()) {
        		    String clust2 = "";
                            tks = anno.get(CoreAnnotations.SentencesAnnotation.class).get(m.sentNum-1).get(CoreAnnotations.TokensAnnotation.class);
                            for(int i = m.startIndex - 1; i < m.endIndex - 1; i++)
                                clust2 += tks.get(i).get(CoreAnnotations.TextAnnotation.class) + " ";
                            clust2 = clust2.trim();
        
                            //don't need the self mention
                            if(clust.equals(clust2))
                                continue;

                            tmpans += clust2 + "|";
                        }
                        ans = tmpans.substring(0, tmpans.length() - 1) + "; ";
                    }
                      
                    mapResults.add(new Tuple2<>(
                            new Tuple2<>(index, func),
                            ans));
                }
                // else if (func.equalsIgnoreCase("dcoref")) {}
            }
            return mapResults.iterator();
        }) //((index, functionality), answer)
                // group by functionality, and then sort by sent-index
                .saveAsTextFile(_args.output);

        spark.stop();

    }
}

/* explicitly construct a Function object:
            new PairFunction<Tuple2<String, Long>, Long, CoreDocument>() {
                @Override
                public Tuple2<Long, CoreDocument> call(Tuple2<String, Long> t) {
                    return new Tuple2<>(t._2(), new CoreDocument(t._1()));
                }
            }
*/

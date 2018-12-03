package ca.uwaterloo.cs651.project;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.coref.data.CorefChain.CorefMention;
import edu.stanford.nlp.naturalli.NaturalLogicAnnotations;
import edu.stanford.nlp.ie.util.RelationTriple;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.*;
import org.kohsuke.args4j.*;

import java.util.*;
import java.util.stream.Collectors;
import java.lang.StringBuilder;

import scala.Tuple2;

public class CoreNLP {
    private static final Logger LOG = Logger.getLogger(CoreNLP.class);

    private static final HashMap<String, String[]> depChain = new HashMap();
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
        supportedFunc.add("openie");

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
        depChain.put("depparse", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma"};
        depChain.put("ner", temp);

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma", "ner"};
        depChain.put("regexner", temp);

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

        temp = new String[]{"tokenize", "ssplit", "pos", "lemma",
                "depparse", "natlog"};
        depChain.put("openie", temp);
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
        LOG.info("functionalities: " + _args.functionality);
        LOG.info("number of mappers: " + _args.numMappers);

        String[] functionalities = _args.functionality.split(",");
        String pipeline_input = buildToDo(functionalities);

        Properties props = new Properties();
        props.setProperty("annotators", pipeline_input);
        props.setProperty("ner.useSUTime", "false");
        if (_args.functionality.contains("regexner"))
            props.put("regexner.mapping", _args.regexner);
        // use faster shift reduce parser
//        props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
        props.setProperty("parse.maxlen", "100");

        SparkSession spark = SparkSession
                .builder()
                .appName("CoreNLP")
                .config("spark.hadoop.validateOutputSpecs", "false")
                .getOrCreate();

        Broadcast<Properties> propsVar = spark.sparkContext().broadcast(
                props, scala.reflect.ClassTag$.MODULE$.apply(Properties.class));
        JavaPairRDD<String, Long> lines = spark.read().textFile(_args.input).javaRDD().zipWithIndex();
        LeftKeyPartitioner partitioner = new LeftKeyPartitioner(functionalities);
        RightKeyComparator comparator = new RightKeyComparator();

        lines
                .repartition(_args.numMappers)
                .mapPartitionsToPair(partition -> {
                    StanfordCoreNLP pipeline = new StanfordCoreNLP(propsVar.getValue());
                    ArrayList<Tuple2<Tuple2<String, Long>, String>> mapResults = new ArrayList<>();

                    while (partition.hasNext()) {
                        Tuple2<String, Long> pair = (Tuple2) partition.next();

                        Long index = pair._2();
                        String line = pair._1();
                        String[] blocks = line.split(": ");
                        String postId = blocks[0].substring(2, blocks[0].length() - 1);
                        String post = blocks[1].substring(1, blocks[1].length() - 2);
                        CoreDocument doc = new CoreDocument(post);
                        Annotation anno = new Annotation(post);
                        pipeline.annotate(doc);
                        pipeline.annotate(anno);

                        for (String func : functionalities) {
                            if (func.equalsIgnoreCase("tokenize")) {
                                String ans = "";
                                for (CoreLabel token : anno.get(CoreAnnotations.TokensAnnotation.class))
                                    ans += token.word() + " ";
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans.substring(0, ans.length() - 1)));
                            } else if (func.equalsIgnoreCase("cleanxml")) {
                                String ans = "";
                                for (CoreLabel word : anno.get(CoreAnnotations.TokensAnnotation.class))
                                    ans += word.toString() + " ";
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans.substring(0, ans.length() - 1)));
                            } else if (func.equalsIgnoreCase("ssplit")) {
                                // String ans = "";
                                StringBuilder ans = new StringBuilder();
                                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                                    ans.append(sentence.toString());
                                    ans.append(System.lineSeparator());
                                }
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index), ans.toString()));
                    /*} else if (func.equalsIgnoreCase("pos")) {
                        String ans = " ";
                        for (CoreLabel token: doc.tokens())
                            ans += token.get(CoreAnnotations.PartOfSpeechAnnotation.class) + " ";
                        mapResults.add(new Tuple2<>(
                                new Tuple2<>(func, index),
                                ans.substring(0, ans.length()-1)));
                    } else if (func.equalsIgnoreCase("lemma")) {
                        String ans = "";
                        for (CoreLabel token: doc.tokens())
                            ans += token.get(CoreAnnotations.LemmaAnnotation.class) + " ";
                        mapResults.add(new Tuple2<>(
                                new Tuple2<>(func, index),
                                ans.substring(0, ans.length()-1)));
                    } else if (func.equalsIgnoreCase("ner")) {
                        String ans = "";
                        for (CoreLabel token: doc.tokens())
                            ans += token.ner() + " ";
                        mapResults.add(new Tuple2<>(
                                new Tuple2<>(func, index),
                                ans.substring(0, ans.length()-1)));*/
                            } else if (func.equalsIgnoreCase("pos")) {
                                String ans = doc.tokens().stream().map(token ->
                                        token.get(CoreAnnotations.PartOfSpeechAnnotation.class))
                                        .collect(Collectors.joining(" "));
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans));
                            } else if (func.equalsIgnoreCase("lemma")) {
                                String ans = doc.tokens().stream().map(token ->
                                        token.get(CoreAnnotations.LemmaAnnotation.class))
                                        .collect(Collectors.joining(" "));
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans));
                            } else if (func.equalsIgnoreCase("ner")) {
                                String ans = doc.tokens().stream().map(token ->
                                        token.ner()).collect(Collectors.joining(" "));
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans));
                            } else if (func.equalsIgnoreCase("regexner")) {
                                String ans = doc.tokens().stream().map(token ->
                                        token.ner()).collect(Collectors.joining(" "));
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans));
                            } else if (func.equalsIgnoreCase("parse")) {
                                String ans = "";
                                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                                    Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
                                    ans += tree.toString() + " ";
                                }
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans.substring(0, ans.length() - 1)));
                            } else if (func.equalsIgnoreCase("depparse")) {
                                String ans = "";
                                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                                    SemanticGraph graph = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
                                    ans += graph.toString() + " ";
                                }
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans.substring(0, ans.length() - 1)));
                            } else if (func.equalsIgnoreCase("sentiment")) {
                                int mainSentiment = -1;
                                String ans = "";
                                int longest = 0;
                                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                                    int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                                    String partText = sentence.toString();
                                    if (partText.length() > longest) {
                                        mainSentiment = sentiment;
                                        longest = partText.length();
                                    }
                                }
                                ans += Integer.toString(mainSentiment);
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index), ans));
                            } else if (func.equalsIgnoreCase("natlog")) {
                                String ans = "";
                                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                                    for (CoreLabel tks : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                                        ans += tks.get(NaturalLogicAnnotations.PolarityAnnotation.class).toString() + " ";
                                    }
                                }

                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans.substring(0, ans.length() - 1)));
                            } else if (func.equalsIgnoreCase("openie")) {
                                // Loop over sentences in the document
                                String ans = "";
                                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                                    // Get the OpenIE triples for the sentence
                                    Collection<RelationTriple> triples = sentence.get(NaturalLogicAnnotations.RelationTriplesAnnotation.class);
                                    // Print the triples
                                    for (RelationTriple triple : triples) {
                                        ans += "(" + triple.confidence + "," + triple.subjectLemmaGloss() + "," + triple.relationLemmaGloss() + "," + triple.objectLemmaGloss() + ")" + " ";
                                    }
                                }
                                mapResults.add(new Tuple2<>(
                                        new Tuple2<>(func, index),
                                        ans.substring(0, ans.length() - 1)));
                            } else if (func.equalsIgnoreCase("coref")) {
                                String ans = "";
                                String tmpans = "";
                                Map<Integer, CorefChain> coref = anno.get(CorefCoreAnnotations.CorefChainAnnotation.class);
                                for (Map.Entry<Integer, CorefChain> entry : coref.entrySet()) {
                                    CorefChain cc = entry.getValue();

                                    //this is because it prints out a lot of self references which aren't that useful
                                    if (cc.getMentionsInTextualOrder().size() <= 1) continue;

                                    CorefMention cm = cc.getRepresentativeMention();
                                    String clust = "";
                                    List<CoreLabel> tks = anno.get(CoreAnnotations.SentencesAnnotation.class).get(cm.sentNum - 1).get(CoreAnnotations.TokensAnnotation.class);
                                    for (int i = cm.startIndex - 1; i < cm.endIndex - 1; i++)
                                        clust += tks.get(i).get(CoreAnnotations.TextAnnotation.class) + " ";
                                    clust = clust.trim();

                                    tmpans += "(" + clust + ":";
                                    for (CorefMention m : cc.getMentionsInTextualOrder()) {
                                        String clust2 = "";
                                        tks = anno.get(CoreAnnotations.SentencesAnnotation.class).get(m.sentNum - 1).get(CoreAnnotations.TokensAnnotation.class);
                                        for (int i = m.startIndex - 1; i < m.endIndex - 1; i++)
                                            clust2 += tks.get(i).get(CoreAnnotations.TextAnnotation.class) + " ";
                                        clust2 = clust2.trim();

                                        //don't need the self mention
                                        if (clust.equals(clust2))
                                            continue;

                                        tmpans += clust2 + "|";
                                    }
                                }
                            }
                        } //end of func enumeration
                    } //end of sentences within a partition
                    return mapResults.iterator();
                })
                .repartitionAndSortWithinPartitions(partitioner, comparator)
                .map(pair -> pair._2())
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

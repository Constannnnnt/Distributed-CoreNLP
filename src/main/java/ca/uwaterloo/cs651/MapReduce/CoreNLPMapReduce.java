package ca.uwaterloo.cs651.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

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

import tl.lin.data.pair.PairOfStringLong;

import java.util.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.lang.StringBuilder;

public class CoreNLPMapReduce extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(CoreNLPMapReduce.class);

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
        temp = new String[] {};
        depChain.put("tokenize", temp);

        temp = new String[] { "tokenize" };
        depChain.put("cleanxml", temp);
        depChain.put("ssplit", temp);

        temp = new String[] { "tokenize", "ssplit" };
        depChain.put("pos", temp);
        depChain.put("parse", temp);

        temp = new String[] { "tokenize", "ssplit", "pos" };
        depChain.put("lemma", temp);
        depChain.put("depparse", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma" };
        depChain.put("ner", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma", "ner" };
        depChain.put("regexner", temp);

        temp = new String[] { "tokenize", "ssplit", "parse" };
        depChain.put("sentiment", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma", "ner", "parse" };
        depChain.put("dcoref", temp);
        depChain.put("coref", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma", "ner", "parse", "depparse" };
        depChain.put("relation", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma", "parse" };
        depChain.put("natlog", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma", "ner", "depparse", "coref" };
        depChain.put("quote", temp);

        temp = new String[] { "tokenize", "ssplit", "pos", "lemma", "depparse", "natlog" };
        depChain.put("openie", temp);
    }

    private static String buildToDo(String[] functionalities) throws IllegalArgumentException {
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

  public static final class CoreNLPMapper extends Mapper<LongWritable, Text, PairOfStringLong, Text> {
    // Reuse objects to save overhead of object creation.
    private static HashMap<String, Float> countMap = new HashMap();
    private static String funcs;
    private static StanfordCoreNLP pipeline;
    private static final PairOfStringLong FUNCIDX = new PairOfStringLong();
    private static final Text ANS = new Text();
    // ArrayList<Tuple2<Tuple2<String, Long>, String>> mapResults = new ArrayList<>();
    String[] functionalities;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        funcs = conf.get("func");

        functionalities = funcs.split(",");
        String pipeline_input = buildToDo(functionalities);

        Properties props = new Properties();
        props.setProperty("annotators", pipeline_input);
        props.setProperty("ner.useSUTime", "false");
        if (funcs.contains("regexner"))
            props.put("regexner.mapping", "regexner.txt");
        props.setProperty("parse.maxlen", "100");
        pipeline = new StanfordCoreNLP(props);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String post = value.toString();
        CoreDocument doc = new CoreDocument(post);
        Annotation anno = new Annotation(post);
        pipeline.annotate(doc);
        pipeline.annotate(anno);
        long k = key.get();

        for (String func : functionalities) {
            if (func.equalsIgnoreCase("tokenize")) {
                String ans = "";
                for (CoreLabel token : anno.get(CoreAnnotations.TokensAnnotation.class)) ans += token.word() + " ";
                if (ans.length() == 0) continue;               
                
                FUNCIDX.set(func, k);
                ANS.set(ans.substring(0, ans.length() - 1));
                context.write(FUNCIDX, ANS);
             } else if (func.equalsIgnoreCase("cleanxml")) {
                String ans = "";
                for (CoreLabel word : anno.get(CoreAnnotations.TokensAnnotation.class)) ans += word.toString() + " ";
		if (ans.length() == 0) continue;
                
                FUNCIDX.set(func, k);
                ANS.set(ans.substring(0, ans.length() - 1));
                context.write(FUNCIDX, ANS);
             } else if (func.equalsIgnoreCase("ssplit")) {
                StringBuilder ans = new StringBuilder();
                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                    ans.append(sentence.toString());
                    ans.append(System.lineSeparator());
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
	    } else if (func.equalsIgnoreCase("pos")) {
                String ans = doc.tokens().stream().map(token -> token.get(CoreAnnotations.PartOfSpeechAnnotation.class)).collect(Collectors.joining(" "));
                FUNCIDX.set(func, k);
                ANS.set(ans);
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("lemma")) {
                String ans = doc.tokens().stream().map(token -> token.get(CoreAnnotations.LemmaAnnotation.class)).collect(Collectors.joining(" "));
                FUNCIDX.set(func, k);
                ANS.set(ans);
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("ner")) {
                String ans = doc.tokens().stream().map(token -> token.ner()).collect(Collectors.joining(" "));
                FUNCIDX.set(func, k);
                ANS.set(ans);
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("regexner")) {
                String ans = doc.tokens().stream().map(token -> token.ner()).collect(Collectors.joining(" "));
                FUNCIDX.set(func, k);
                ANS.set(ans);
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("parse")) {
                StringBuilder ans = new StringBuilder();
                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree tree = sentence.get(TreeCoreAnnotations.TreeAnnotation.class);
                    ans.append(tree.toString());
                    ans.append(System.lineSeparator());
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("depparse")) {
                StringBuilder ans = new StringBuilder();
                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                    SemanticGraph graph = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);
                    ans.append(graph.toString());
                    ans.append(System.lineSeparator());
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("sentiment")) {
                StringBuilder ans = new StringBuilder();
                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                    ans.append(Integer.toString(sentiment));
                    ans.append(System.lineSeparator());
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("natlog")) {
                StringBuilder ans = new StringBuilder();
                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                    for (CoreLabel tks : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        ans.append(tks.get(NaturalLogicAnnotations.PolarityAnnotation.class).toString() + " ");
                    }
                    ans.append(System.lineSeparator());
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("openie")) {
                StringBuilder ans = new StringBuilder();
                for (CoreMap sentence : anno.get(CoreAnnotations.SentencesAnnotation.class)) {
                    // Get the OpenIE triples for the sentence
                    Collection<RelationTriple> triples = sentence.get(NaturalLogicAnnotations.RelationTriplesAnnotation.class);
                    // Print the triples
                    for (RelationTriple triple : triples) {
                        ans.append("(" + triple.confidence + "," + triple.subjectLemmaGloss() + "," + triple.relationLemmaGloss() + "," + triple.objectLemmaGloss() + ")");
                        ans.append(System.lineSeparator());
                    }
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
            } else if (func.equalsIgnoreCase("coref")) {
                StringBuilder ans = new StringBuilder();
                Map<Integer, CorefChain> coref = anno.get(CorefCoreAnnotations.CorefChainAnnotation.class);
                for (Map.Entry<Integer, CorefChain> entry : coref.entrySet()) {
                    String tmpans = "";
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
                        if (clust.equals(clust2)) continue;
                        tmpans += clust2 + "|";
                    }
                    tmpans = tmpans.substring(0, tmpans.length() - 1) + ")";
                    ans.append(tmpans);
                    ans.append(System.lineSeparator());
                }
                FUNCIDX.set(func, k);
                ANS.set(ans.toString());
                context.write(FUNCIDX, ANS);
            }
        }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private CoreNLPMapReduce() {}

  private static final class MRArgs {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-functionality", metaVar = "[func]", required = true, usage = "property of the corenlp")
    String func;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final MRArgs args = new MRArgs();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + CoreNLPMapReduce.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - functionality: " + args.func);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(CoreNLPMapReduce.class.getSimpleName());
    job.setJarByClass(CoreNLPMapReduce.class);

    job.getConfiguration().set("func", args.func);

    // job.setNumMapTasks(args.numMappers);
    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfStringLong.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfStringLong.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(CoreNLPMapper.class);
    // job.setCombinerClass(MyReducer.class);
    // job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CoreNLPMapReduce(), args);
  }
}

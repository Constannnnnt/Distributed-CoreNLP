package ca.uwaterloo.cs651.project;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.pipeline.CoreNLPProtos.Sentiment;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.simple.*;
import edu.stanford.nlp.util.Quadruple;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class CoreNLP {
    private static final Logger LOG = Logger.getLogger(CoreNLP.class);
    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;
    }
    
    public static void main(String[] args) { 
        // Create a document. No computation is done yet.
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
        LOG.info(" - input path: " + _args.input);
        // LOG.info(" - output path: " + args.output);
        // LOG.info(" - number of reducers: " + args.numReducers);

        Document doc = new Document("add your text here! It can contain multiple sentences.");
        for (Sentence sent : doc.sentences()) {  // Will iterate over two sentences
            // We're only asking for words -- no need to load any models yet
            System.out.println("The second word of the sentence '" + sent + "' is " + sent.word(1));
            // When we ask for the lemma, it will load and run the part of speech tagger
            System.out.println("The third lemma of the sentence '" + sent + "' is " + sent.lemma(2));
            // When we ask for the parse, it will load and run the parser
            System.out.println("The parse of the sentence '" + sent + "' is " + sent.parse());
            // ...
        }
    }
}

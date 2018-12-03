package ca.uwaterloo.cs651.project;


import org.kohsuke.args4j.Option;

public class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-regexner", metaVar = "[path]", usage = "regexner path")
    String regexner;

    @Option(name = "-functionality", metaVar = "[listOfString]", required = true,
            usage = "required funtionalities, separated by comma")
    String functionality;

    @Option(name = "-mappers", metaVar = "[num]", usage = "number of mappers")
    int numMappers = 1;
}
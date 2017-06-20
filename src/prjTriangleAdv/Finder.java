package prjTriangleAdv;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hadoop jar TriangleWiki.jar prjTriangleWiki.Finder 1 INPUT/ttter/twitter-big-sample.txt OUTPUT/twitterBig
//hadoop jar Triangle.jar prjTriangle.TriangleFinder 1 INPUT/twitter/twitter-small.txt OUTPUT/twitter

public class Finder extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private Path partialDir;
    private int b;


    public int run(String[] args) throws Exception {
        Runner runner = new Runner(outputDir, inputPath, partialDir, b, this);
        runner
//                .runCounter()
//                .runDegree()
//                .runHH()

                .runOthers1(MapperOthers1.class,ReducerOthers1.class)
                .runOthers2(MapperOthers2.class, ReducerOthers2.class)
        ;
        return 1;
    }

    public Finder(Path inputPath, Path outputDir, Path partialDir, int b) {
        this.b = b;
        this.inputPath = inputPath;
        this.outputDir = outputDir;
        this.partialDir = partialDir;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "output dir");
        output.setRequired(true);
        options.addOption(output);

        Option bucket = new Option("b", "bucket", true, "number of buckets");
        output.setRequired(true);
        options.addOption(bucket);
        Option libjars = new Option("libjars", "libjars", true, "libjars");
        libjars.setRequired(true);
        options.addOption(libjars);


        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
            return;
        }
        Path inputPath = new Path(cmd.getOptionValue("input"));
        Path outputDir = new Path(cmd.getOptionValue("output"));
        Path partialDir = new Path("_partial/" + cmd.getOptionValue("output"));
        int b = new Integer(cmd.getOptionValue("bucket"));


        int res = ToolRunner.run(new Configuration(), new Finder(inputPath, outputDir, partialDir, b), args);
        System.exit(res);
    }

}

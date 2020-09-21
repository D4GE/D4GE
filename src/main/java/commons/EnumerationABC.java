package commons;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * PSE class for all the enumeration programs. It defines the program entry |run| and a constructor
 * to parse the argument.
 */
public abstract class EnumerationABC implements Serializable {

  protected static JavaSparkContext jsc;
  protected static FileSystem fileSystem;

  // Absolute path to the working directory containing all the files.
  protected String inputDir;
  // Basename of the original graph.
  protected String originalGraphName;
  // Basename of the graph transpose.
  protected String transposeGraphName;
  // Partition the graph into choose(numColors, 2)+choose(numColors, 3) sub-problems.
  protected int numColors;
  // Indicates whether apply pre-processing.
  protected boolean preProc = false;
  // Filename of the enumeration results.
  protected static final String resultName = "/results.txt";

  protected String queryGraph;
  protected int numInputSplit;

  public EnumerationABC(String[] args) throws URISyntaxException, IOException {
    Options options = new Options();

    final String inputDirArg = "input-dir",
        originalGraphArg = "original",
        transposeGraphArg = "transpose",
        numColorArg = "num-colors",
        enablePreproc = "enable-preproc",
        queryGraph = "query-graph",
        numInputSplit = "num-split";

    Option option =
        new Option(
            "d", inputDirArg, true, "Path to input directory. Start with hdfs:// or file://");
    option.setRequired(true);
    options.addOption(option);

    option = new Option("o", originalGraphArg, true, "Name of the original graph file.");
    option.setRequired(true);
    options.addOption(option);

    option =
        new Option(
            "t",
            transposeGraphArg,
            true,
            "Name of the transpose graph file. Transpose is optional, and will be ignored if not required by the algorithm.");
    option.setRequired(false);
    option.setArgs(1);
    options.addOption(option);

    option = new Option("c", numColorArg, true, "Number of colors.");
    option.setRequired(true);
    options.addOption(option);

    option = new Option("p", enablePreproc, false, "Enable the preprocessing (for Triad and 4G).");
    option.setRequired(false);
    option.setArgs(0);
    options.addOption(option);

    option = new Option("q", queryGraph, true, "Query graph (for Y4G_mod). Default: 'all'");
    option.setRequired(false);
    option.setArgs(1);
    options.addOption(option);

    option =
        new Option(
            "l", numInputSplit, true, "Number of splits of the WebGraph input. Default: 100");
    option.setRequired(false);
    option.setArgs(1);
    options.addOption(option);

    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("utility-name", options);
      System.exit(1);
    }

    inputDir = cmd.getOptionValue(inputDirArg);
    originalGraphName = cmd.getOptionValue(originalGraphArg);
    transposeGraphName = cmd.getOptionValue(transposeGraphArg);
    numColors = Integer.parseInt(cmd.getOptionValue(numColorArg));

    if (cmd.hasOption(queryGraph)) this.queryGraph = cmd.getOptionValue(queryGraph);
    else this.queryGraph = "all";

    if (cmd.hasOption(numInputSplit))
      this.numInputSplit = Integer.parseInt(cmd.getOptionValue(numInputSplit));
    else this.numInputSplit = 100;

    if (cmd.hasOption(enablePreproc)) preProc = true;

    SparkConf conf =
        new SparkConf()
            .setAppName(
                String.format(
                    "%s-%s-%d-%b-%s",
                    this.getClass().getSimpleName(),
                    originalGraphName,
                    numColors,
                    preProc,
                    this.queryGraph));

    jsc = new JavaSparkContext(conf);
    jsc.setLogLevel("WARN"); // silence some noise outputs.

    fileSystem = FileSystem.get(new URI(inputDir), jsc.hadoopConfiguration());
  }

  /**
   * Entry point of all the enumeration programs.
   *
   * @throws Exception All uncaught exceptions.
   */
  protected abstract void run() throws Exception;

  /**
   * Partition the graph input. For triad and cycle-triangle counting, implement the partitioning on
   * the original and the transpose.
   */
  protected abstract void partitionGraph() throws IOException;
}

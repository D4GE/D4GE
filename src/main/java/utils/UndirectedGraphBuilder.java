package utils;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.IncrementalImmutableSequentialGraph;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Generate an undirected, pre-processed edge list combining the original edge list and transpose
 * edge list.
 *
 * <p>Rule of pre-processing: emit edge(u, v) iff deg(u) < deg(v), or deg(u) == deg(v) && u < v.
 *
 * <p>Pre-processing essentially redistributes the adjacency list of the graph. It makes the adj
 * list more even/smooth (draw a small example to visualize it).
 */
public class UndirectedGraphBuilder {

  final int[] outDegrees;
  String baseName;
  String transposeName;
  final int N;
  private final ImmutableGraph G;
  private final ImmutableGraph Gt;
  boolean preProc = false;

  UndirectedGraphBuilder(String[] args) throws IOException {
    readArgs(args);
    G = ImmutableGraph.loadMapped(baseName);
    Gt = ImmutableGraph.loadMapped(transposeName);
    N = Math.max(G.numNodes(), Gt.numNodes());
    outDegrees = new int[N];
    for (int u = 0; u < G.numNodes(); ++u) {
      outDegrees[u] += G.outdegree(u);
    }
    for (int u = 0; u < Gt.numNodes(); ++u) {
      outDegrees[u] += G.outdegree(u);
    }
  }

  private void readArgs(String[] args) {
    Options options = new Options();

    Option option = new Option("o", "original", true, "Basename of the original graph file.");
    option.setRequired(true);
    options.addOption(option);

    option = new Option("t", "transpose", true, "Basename of the transpose graph file.");
    option.setRequired(true);
    options.addOption(option);

    option = new Option("p", "enable-preproc", false, "Enable the preprocessing");
    option.setRequired(false);
    option.setArgs(0);
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

    baseName = cmd.getOptionValue("original");
    transposeName = cmd.getOptionValue("transpose");
    if (cmd.hasOption("enable-preproc")) {
      preProc = true;
    }
  }

  /**
   * Merge two sorted array in sorted order, and remove the duplicates. The merged elements are
   * stored in the first k elements of |output|.
   */
  private int mergeListsDedup(int[] a, int[] b, int aLen, int bLen, int[] output, int u) {
    int i = 0, j = 0, k = 0;

    while (i < aLen || j < bLen) {
      int v;
      if (i < aLen && j < bLen) {
        v = a[i] < b[j] ? a[i++] : b[j++];
      } else if (i < aLen) {
        v = a[i++];
      } else {
        v = b[j++];
      }

      if (k - 1 >= 0 && output[k - 1] == v) {
        continue;
      }
      if (u < v) {
        output[k++] = v;
      }
    }
    return k;
  }

  /**
   * Merge two sorted array in sorted order, remove the duplicates, and apply preprocessing. The
   * merged elements are stored in the first k elements of |output|.
   */
  private int mergeListsDedupAndPreproc(int[] a, int[] b, int aLen, int bLen, int[] output, int u) {
    int i = 0, j = 0, k = 0;

    while (i < aLen || j < bLen) {
      int v;
      if (i < aLen && j < bLen) {
        v = a[i] < b[j] ? a[i++] : b[j++];
      } else if (i < aLen) {
        v = a[i++];
      } else {
        v = b[j++];
      }

      if (k - 1 >= 0 && output[k - 1] == v) {
        continue;
      }

      int du = outDegrees[u];
      int dv = outDegrees[v];
      if (dv > du || (dv == du && v > u)) {
        output[k++] = v;
      }
    }
    return k;
  }

  /** Write preprocessed undirected graph. */
  private void writePreproc() throws InterruptedException, ExecutionException {
    final IncrementalImmutableSequentialGraph destGraph = new IncrementalImmutableSequentialGraph();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Void> future =
        executor.submit(
            () -> {
              BVGraph.store(destGraph, baseName + "-u-preproc");
              return null;
            });

    for (int u = 0; u < N; ++u) {
      // see
      // http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/ImmutableGraph.html#successorArray(int)
      // on why limit the size of the array.
      int[] output = new int[G.outdegree(u) + Gt.outdegree(u)];
      int k =
          mergeListsDedupAndPreproc(
              G.successorArray(u),
              Gt.successorArray(u),
              G.outdegree(u),
              Gt.outdegree(u),
              output,
              u);
      destGraph.add(output, 0, k);
    }

    destGraph.add(IncrementalImmutableSequentialGraph.END_OF_GRAPH);
    future.get();
    executor.shutdown();
  }

  /** Write undirected graph without preprocessing */
  private void write() throws InterruptedException, ExecutionException {
    final IncrementalImmutableSequentialGraph destGraph = new IncrementalImmutableSequentialGraph();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Void> future =
        executor.submit(
            () -> {
              BVGraph.store(destGraph, baseName + "-u");
              return null;
            });

    for (int u = 0; u < N; ++u) {
      // see
      // http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/ImmutableGraph.html#successorArray(int)
      // on why limit the size of the array.
      int[] output = new int[G.outdegree(u) + Gt.outdegree(u)];
      int k =
          mergeListsDedup(
              G.successorArray(u),
              Gt.successorArray(u),
              G.outdegree(u),
              Gt.outdegree(u),
              output,
              u);
      destGraph.add(output, 0, k);
    }

    destGraph.add(IncrementalImmutableSequentialGraph.END_OF_GRAPH);
    future.get();
    executor.shutdown();
  }

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    UndirectedGraphBuilder builder = new UndirectedGraphBuilder(args);
    if (builder.preProc) {
      System.out.println("Generate undirected graph with preprocessing...");
      builder.writePreproc();
    } else {
      System.out.println("Generate undirected graph without preprocessing...");
      builder.write();
    }
  }
}

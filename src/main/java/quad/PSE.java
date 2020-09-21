package quad;

import commons.Commons;
import commons.map.EdgesetOutputFormat;
import commons.map.IntIntArrayHashMap;
import de.l3s.mapreduce.webgraph.io.IntArrayWritable;
import de.l3s.mapreduce.webgraph.io.WebGraphInputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple4;

public class PSE extends QuadABC {

  public PSE(String[] args) throws IOException, URISyntaxException {
    super(args);
  }

  @Override
  protected void generateSeed() throws IOException {
    List<List<Long>> problemGroups = getProblemGroups(numColors);
    Collections.shuffle(problemGroups);
    FSDataOutputStream seedOut = fileSystem.create(new Path(inputDir + Commons.SEEDFILE), true);
    for (List<Long> group : problemGroups) {
      String encoded = group.stream().map(Object::toString).collect(Collectors.joining(","));
      seedOut.writeBytes(encoded + "\n");
    }
    seedOut.close();
  }

  /**
   * Implementation of PSE paper's "subproblem group". Acknowledge that PTE's styled subproblems
   * have overlap, hence to de-duplicate the counts across these subproblems, group the overlapped
   * subproblems
   *
   * @param numColors The number of colors.
   * @return A list of problem groups. Each group consists of one or more subproblem, represented in
   *     an integer form.
   */
  private static List<List<Long>> getProblemGroups(int numColors) {
    assert numColors < 63;
    Random rdm = new Random();

    // Get all problems with size 3.
    List<Long> problemsRhoMinusOne = new ArrayList<>();
    backtrack(0, 0, numColors, 3, problemsRhoMinusOne);

    // Add size-3 problems into problemGroups
    List<List<Long>> problemGroups = new ArrayList<>();
    for (Long aLong : problemsRhoMinusOne)
      problemGroups.add(new ArrayList<>(Collections.singletonList(aLong)));

    // q can be 2 and 1.
    // For each q-sized problem, add it to a dominating group. We use the min length of the
    // dominating group length as the heuristic to determine which dominating group to add, as a
    // single problem can be dominated by more than one group.
    for (int q = 2; q >= 1; --q) {
      List<Long> problems = new ArrayList<>();
      backtrack(0, 0, numColors, q, problems);

      for (long problem : problems) {
        List<Integer> candidateIndices = new ArrayList<>();
        int minDominateGroupSize = Integer.MAX_VALUE;
        for (List<Long> problemGroup : problemGroups) {
          // If any of the 3 problems dominates the current problem, update minDominateGroupSize.
          if ((problemGroup.get(0) & problem) == problem)
            minDominateGroupSize = Math.min(minDominateGroupSize, problemGroup.size());
        }
        // Check all groups, if the current group dominates the problem of q vertices, and the
        // current group size is minDominateGroupSize, then this group is a candidate to add the
        // current problem.
        for (int i = 0; i < problemGroups.size(); ++i) {
          List<Long> problemGroup = problemGroups.get(i);
          // If any of the rho-1 problems dominates the current problem, update
          // minDominateGroupSize.
          if ((problemGroup.get(0) & problem) == problem
              && problemGroup.size() == minDominateGroupSize) candidateIndices.add(i);
        }
        // Break the tie randomly.
        int j = candidateIndices.get(rdm.nextInt(candidateIndices.size()));
        problemGroups.get(j).add(problem);
      }
    }

    // Now, add the 4-sized problems. 4-sized problems do not dominate any other problems.
    List<Long> problemsRho = new ArrayList<>();
    backtrack(0, 0, numColors, 4, problemsRho);
    for (Long problem : problemsRho)
      problemGroups.add(new ArrayList<>(Collections.singletonList(problem)));

    // Sanity check
    for (List<Long> problemGroup : problemGroups) {
      assert problemGroup.size() <= 3;
      for (Long problem : problemGroup) {
        assert problem == (problem & problemGroup.get(0));
      }
    }
    return problemGroups;
  }

  /**
   * Backtrack to fill out numColor bits with numVtx number of 1s. Start filling out from right to
   * left.
   *
   * <p>For example: backtrack(0, 0, 5, 4, {}) returns [15, 23, 27, 29, 30]
   *
   * <p>15 in binary: 01111 23: 10111 27: 11011 29: 11101 30: 11110
   *
   * <p>Since numColors is limited to 62, the left shift of 1L << i will not overflow long.
   *
   * @param currVal The integer representation of assigning numVtx 1s to currVal of width numColors.
   * @param currIdx The position of next bit to assign a 1.
   * @param numColors The width of currVal.
   * @param numVtx The total number of 1s to assign.
   * @param output Output list to store the assigned integers.
   */
  private static void backtrack(
      long currVal, int currIdx, int numColors, int numVtx, List<Long> output) {
    if (Long.bitCount(currVal) == numVtx) {
      output.add(currVal);
      return;
    }
    for (int i = currIdx; i < numColors; ++i)
      backtrack(currVal | (1L << i), i + 1, numColors, numVtx, output);
  }

  public static void main(String[] args) throws Exception {
    PSE app = new PSE(args);
    app.run();
  }

  @Override
  protected void preprocessAndPartition() throws IOException {
    // Read the symmetrized graph and apply preprocessing, then generate the cut version.
    JavaPairRDD<IntWritable, IntArrayWritable> symm = readGraph(inputDir + originalGraphName);
    JavaPairRDD<Integer, int[]> preprocessed = Commons.preprocessSingle(symm);
    fileSystem.delete(new Path(inputDir + symmPath), true);
    Commons.partitionPreprocGraph(preprocessed, numColors, inputDir + symmPath);
  }

  @Override
  protected void partitionGraph() throws IOException {
    JavaPairRDD<IntWritable, IntArrayWritable> symm = readGraph(inputDir + originalGraphName);
    fileSystem.delete(new Path(inputDir + symmPath), true);
    Commons.partitionUnprocGraph(symm, numColors, inputDir + symmPath);
  }

  protected JavaPairRDD<IntWritable, IntArrayWritable> readGraph(String fullPath)
      throws IOException {
    WebGraphInputFormat.setNumberOfSplits(jsc.hadoopConfiguration(), this.numInputSplit);
    WebGraphInputFormat.setBasename(jsc.hadoopConfiguration(), fullPath);
    return jsc.newAPIHadoopRDD(
        jsc.hadoopConfiguration(),
        WebGraphInputFormat.class,
        IntWritable.class,
        IntArrayWritable.class);
  }

  @Override
  public Tuple4<QuadGraphletAccumulator, Long, List<Double>, List<Double>> enumerateQuad(
      Broadcast<SerializableWritable<Configuration>> hadoopConf) throws IOException {
    List<String> colorEncodings = Commons.readSeedFile(fileSystem, inputDir);

    QuadGraphletAccumulator acc = new QuadGraphletAccumulator(jsc);
    LongAccumulator kiloBytesRead = jsc.sc().longAccumulator();
    CollectionAccumulator<Double> enumTimes = jsc.sc().collectionAccumulator();
    CollectionAccumulator<Double> networkReadTimes = jsc.sc().collectionAccumulator();

    jsc.parallelize(colorEncodings)
        .foreach(
            line -> {
              FileSystem fs = FileSystem.get(new URI(inputDir), hadoopConf.value().value());
              long[] problemGroup = Stream.of(line.split(",")).mapToLong(Long::parseLong).toArray();
              // First problem of each group dominates the entire group.
              long dominateProblem = problemGroup[0];
              // Count the number of 1s in the dominate problem. The count is the number of colors
              // of the group.
              int numGroupColors = Long.bitCount(dominateProblem);
              // Restore the colors from binary representation of the dominate problem.
              // dominateProblem 11010 -> groupColors [1, 3, 4]
              int[] groupColors = new int[numGroupColors];
              for (int clr = 0, j = 0; clr < numColors; ++clr)
                if ((dominateProblem & (1 << clr)) != 0) groupColors[j++] = clr;

              IntIntArrayHashMap Gsymm = new IntIntArrayHashMap();

              long t = System.currentTimeMillis();

              loadEdgeset(groupColors, numGroupColors, Gsymm, fs);

              kiloBytesRead.add(IntStream.of(Gsymm.get(Commons.SIZE_KEY)).sum());
              Gsymm.remove(Commons.SIZE_KEY);

              long tt = System.currentTimeMillis();
              System.out.printf(
                  "group %s: |loadNeighbourHashMap| takes %.4f seconds.%n",
                  Arrays.toString(problemGroup), (tt - t) / 1000.0);
              networkReadTimes.add((tt - t) / 1000.0);

              // Consider problem group [26, 10, 24]
              // 26=11010, 10=01010, 24=11000
              // groupColors=[1,3,4], generated from the dominate problem of this group, 26.
              // Now re-encode these three problems using groupColors:
              // 26=111, 10=110, 24=011
              long[] encodedProblems =
                  Arrays.stream(problemGroup)
                      .map(
                          problem -> {
                            long encoded = 0L;
                            for (int i = 0; i < groupColors.length; i++) {
                              encoded |= ((problem >>> groupColors[i]) & 1) << i;
                            }
                            return encoded;
                          })
                      .toArray();

              IEnumerator counter;
              if (queryGraph.equals("g8")) { // k4
                counter = new K4Enumerator(Gsymm, groupColors, encodedProblems, numColors);
              } else {
                counter =
                    new QuadGraphletEnumerator(
                        Gsymm, groupColors, encodedProblems, numColors, queryGraph);
              }
              Tuple2<Long[], Long[]> result = counter.countQuadGraphlet();

              acc.accumulateCounts(result._1);
              acc.accumulateDups(result._2);

              enumTimes.add((System.currentTimeMillis() - tt) / 1000.0);
              System.out.printf(
                  "group %s: found %s%n",
                  Arrays.toString(problemGroup), Arrays.toString(result._1));
            });
    return new Tuple4<>(acc, kiloBytesRead.value(), enumTimes.value(), networkReadTimes.value());
  }

  protected void loadEdgeset(
      int[] groupColors, int numGroupColors, IntIntArrayHashMap Gsymm, FileSystem fs)
      throws IOException {
    for (int c1 : groupColors) {
      for (int c2 : groupColors) {
        // Do not load Eii if the group has 4 colors
        if (c1 == c2 && numGroupColors == 4) continue;
        IntIntArrayHashMap edgelistSymm =
            Commons.loadEdgeset(
                inputDir + symmPath + String.format(EdgesetOutputFormat.outputFormat, c1, c2), fs);
        edgelistSymm.forEachEntry(
            (int vtx, int[] nbs) -> {
              Gsymm.put(vtx, Commons.merge(Gsymm.get(vtx), nbs));
              return true;
            });
      }
    }
  }
}

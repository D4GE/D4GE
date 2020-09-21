package quad;

import commons.Commons;
import commons.map.EdgesetOutputFormat;
import commons.map.IntIntArrayHashMap;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SerializableWritable;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple4;

public class CD extends PSE {

  public CD(String[] args) throws IOException, URISyntaxException {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    CD app = new CD(args);
    app.run();
  }

  protected boolean belongTo(int i, int j, int k, int l, int[] problem) {
    if (i == j && j == k && k == l) {
      if (problem.length == 2) {
        if (problem[0] + 1 == problem[1] && problem[0] == i) return true;
        return problem[0] == 0 && problem[1] == numColors - 1 && i == numColors - 1;
      }
      return false;
    }
    Set<Integer> s = new TreeSet<>();
    s.add(i);
    s.add(j);
    s.add(k);
    s.add(l);
    Integer[] ss = s.toArray(new Integer[0]);
    if (ss.length != problem.length) return false;
    for (int idx = 0; idx < ss.length; ++idx) if (ss[idx] != problem[idx]) return false;
    return true;
  }

  protected List<int[]> generateConfigs(int[] problem) {
    List<int[]> configs = new ArrayList<>();
    for (int i = 0; i < numColors; ++i)
      for (int j = 0; j < numColors; ++j)
        for (int k = 0; k < numColors; ++k)
          for (int l = 0; l < numColors; ++l)
            if (belongTo(i, j, k, l, problem)) configs.add(new int[] {i, j, k, l});
    return configs;
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
              int[] problem = Stream.of(line.split(",")).mapToInt(Integer::parseInt).toArray();
              List<int[]> configs = generateConfigs(problem);

              System.out.printf("problem %s: ", Arrays.toString(problem));
              for (int[] config : configs) System.out.printf("%s ", Arrays.toString(config));
              System.out.println();

              Map<String, IntIntArrayHashMap> Gsymm = new HashMap<>();

              long t = System.currentTimeMillis();

              // load edgeset
              for (int[] config : configs) {
                for (int i = 0; i < config.length; ++i) {
                  int from = config[i];
                  for (int j = i + 1; j < config.length; ++j) {
                    int to = config[j];
                    String key = String.format("%d-%d", from, to);
                    if (Gsymm.get(key) != null) continue;
                    // load symm
                    IntIntArrayHashMap edgesetSymm =
                        Commons.loadEdgeset(
                            inputDir
                                + symmPath
                                + String.format(EdgesetOutputFormat.outputFormat, from, to),
                            fs);
                    kiloBytesRead.add(IntStream.of(edgesetSymm.get(Commons.SIZE_KEY)).sum());
                    edgesetSymm.remove(Commons.SIZE_KEY);
                    Gsymm.put(key, edgesetSymm);
                  }
                }
              }

              long tt = System.currentTimeMillis();
              System.out.printf(
                  "problem %s: load edgeset takes %.4f seconds.%n",
                  Arrays.toString(problem), (tt - t) / 1000.0);
              networkReadTimes.add((tt - t) / 1000.0);

              long[] temp = new long[9];
              for (int[] config : configs) {
                long[] cnt;
                if (queryGraph.equals("g8")) {
                  cnt = new K4EnumeratorNoFilter(Gsymm, config).countQuadGraphlet();
                } else {
                  cnt =
                      new QuadGraphletEnumeratorNoFilter(Gsymm, config, queryGraph)
                          .countQuadGraphlet();
                }
                for (int i = 0; i < temp.length; ++i) temp[i] += cnt[i];
              }
              System.out.printf(
                  "problem %s: found %s.%n", Arrays.toString(problem), Arrays.toString(temp));
              acc.accumulateCounts(temp);

              enumTimes.add((System.currentTimeMillis() - tt) / 1000.0);
            });
    return new Tuple4<>(acc, kiloBytesRead.value(), enumTimes.value(), networkReadTimes.value());
  }

  @Override
  protected void generateSeed() throws IOException {
    Commons.createSeedFileForQuad(numColors, fileSystem, inputDir);
  }
}

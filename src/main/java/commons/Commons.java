package commons;

import commons.map.EdgesetOutputFormat;
import commons.map.IntIntArrayHashMap;
import commons.map.IntTIntArrayListHashMap;
import de.l3s.mapreduce.webgraph.io.IntArrayWritable;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Commons implements Serializable {

  // dummy key for storing the size of kilobyte read from filesystem.
  public static final int SIZE_KEY = -42;
  // the name of seed file.
  public static final String SEEDFILE = "sub-problems.txt";

  /**
   * Read and return seed file as an ArrayList. The list must have the size of choose(#color, 2) +
   * choose(#color, 3).
   *
   * @param fs Filesystem abstraction.
   * @param fullPath The absolute full path to store the seed file.
   * @throws IOException Thrown if the specified full path cannot be found.
   */
  public static ArrayList<String> readSeedFile(FileSystem fs, String fullPath) throws IOException {
    ArrayList<String> lines = new ArrayList<>();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(fs.open(new Path(fullPath + SEEDFILE))))) {
      String line = br.readLine();
      while (line != null) {
        lines.add(line);
        line = br.readLine();
      }
    }
    System.out.printf("%s has %d lines%n", fullPath + SEEDFILE, lines.size());
    return lines;
  }

  /**
   * Creat a seed file for 4-node graphlet counting - a random shuffled text file with all possible
   * sub-problems encoded. There are choose(#color, 2) + choose(#color, 3) + choose(#color, 4)
   * sub-problems.
   *
   * @param numColors Number of partitions.
   * @param fs Filesystem abstraction.
   * @param fullPath The absolute full path to store the seed file.
   * @throws IOException Thrown if the specified full path cannot be found.
   */
  public static void createSeedFileForQuad(int numColors, FileSystem fs, String fullPath)
      throws IOException {
    ArrayList<String> seedList = new ArrayList<>();
    for (int i = 0; i < numColors; ++i)
      for (int j = i + 1; j < numColors; ++j)
        for (int k = j + 1; k < numColors; ++k)
          for (int l = k + 1; l < numColors; ++l)
            seedList.add(String.format("%d,%d,%d,%d\n", i, j, k, l));

    for (int i = 0; i < numColors; ++i)
      for (int j = i + 1; j < numColors; ++j)
        for (int k = j + 1; k < numColors; ++k) seedList.add(String.format("%d,%d,%d\n", i, j, k));

    for (int i = 0; i < numColors; ++i)
      for (int j = i + 1; j < numColors; ++j) seedList.add(String.format("%d,%d\n", i, j));

    Collections.shuffle(seedList);
    FSDataOutputStream seedOut = fs.create(new Path(fullPath + SEEDFILE), true);
    for (String s : seedList) {
      seedOut.writeBytes(s);
    }
    seedOut.close();
  }

  /** Merge two sorted array in sorted order. */
  public static int[] merge(int[] a, int[] b) {
    if (a == null) a = new int[0];
    if (b == null) b = new int[0];
    int[] merged = new int[a.length + b.length];
    int i = a.length - 1;
    int j = b.length - 1;
    int k = merged.length;
    while (k > 0) {
      merged[--k] = j < 0 || i >= 0 && a[i] >= b[j] ? a[i--] : b[j--];
    }
    return merged;
  }

  /**
   * Read specified partitioned edgesets. To be called by a worker.
   *
   * @param neighbourMapPath Absolute path to the partitioned edgesets.
   * @param fs Filesystem abstraction.
   * @return Sub-problem loaded in a Trove |IntIntArrayHashMap|.
   * @throws IOException Thrown if the specified sub-problem cannot be found.
   */
  public static IntIntArrayHashMap loadEdgeset(String neighbourMapPath, FileSystem fs)
      throws IOException {
    IntIntArrayHashMap edgeset = new IntIntArrayHashMap();
    try {
      Path p = new Path(neighbourMapPath);
      FSDataInputStream fin = fs.open(p);
      edgeset.in(fin);
      // getLen return bytes of a file in long
      edgeset.put(SIZE_KEY, new int[] {(int) (fs.getFileStatus(p).getLen() / 1024)});
    } catch (FileNotFoundException ignored) {
      edgeset.put(SIZE_KEY, new int[] {0});
      System.out.printf("%s not found, ignored\n", neighbourMapPath);
    }
    return edgeset;
  }

  /**
   * Partition the un-preprocessed graph and save to filesystem. The public interface flattens the
   * adjacency list and emits a list of edges; then it invokes a private method to further partition
   * these edges.
   *
   * @param input JavaRDD of ((a, (b, c, d, ...)), (e, (f, g, h, ...)) ...) where a->b, a->c, e->f
   *     etc are edges.
   * @param numColors Number of partitions.
   * @param fullPath Absolute full path to save the partitioned sub-graphs.
   * @throws IOException IOException thrown if unable to store sub-graphs to |fullPath|.
   */
  public static void partitionPreprocGraph(
      JavaPairRDD<Integer, int[]> input, int numColors, String fullPath) throws IOException {
    JavaPairRDD<Integer, Integer> flat =
        input.flatMapToPair(
            pair -> {
              List<Tuple2<Integer, Integer>> retVal = new ArrayList<>();
              for (int nb : pair._2) retVal.add(new Tuple2<>(pair._1, nb));
              return retVal.iterator();
            });
    partitionGraph(flat, numColors, fullPath);
  }

  /**
   * Partition the un-preprocessed graph and save to filesystem. The public interface flattens the
   * adjacency list and emits a list of edges; then it invokes a private method to further partition
   * these edges.
   *
   * @param input JavaRDD of ((a, (b, c, d, ...)), (e, (f, g, h, ...)) ...) where a->b, a->c, e->f
   *     etc are edges.
   * @param numColors Number of partitions.
   * @param fullPath Absolute full path to save the partitioned sub-graphs.
   * @throws IOException IOException thrown if unable to store sub-graphs to |fullPath|.
   */
  public static void partitionUnprocGraph(
      JavaPairRDD<IntWritable, IntArrayWritable> input, int numColors, String fullPath)
      throws IOException {
    JavaPairRDD<Integer, Integer> flat =
        input
            // Flat the <node, List[neighbours]> to <edgePair>
            .flatMapToPair(
            writablePair -> {
              List<Tuple2<Integer, Integer>> retVal = new ArrayList<>();
              for (int nb : writablePair._2.values()) {
                retVal.add(new Tuple2<>(writablePair._1.get(), nb));
              }
              return retVal.iterator();
            });
    partitionGraph(flat, numColors, fullPath);
  }

  /**
   * Partitions the RDD of edge tuples and store into the filesystem.
   *
   * @param input RDD of ((u, v), (w, x), (y, z) ... ) where u->v, w->x and y->z are edges.
   * @param numColors Number of colors (rho).
   * @param fullPath Absolute full path to the directory storing the partitioned edges.
   * @throws IOException Thrown if the specified file path cannot be created.
   */
  public static void partitionGraph(
      JavaPairRDD<Integer, Integer> input, int numColors, String fullPath) throws IOException {
    JavaPairRDD<Tuple2<Integer, Integer>, IntTIntArrayListHashMap> partitionMap =
        // map each edge tuple <u, v> to <<i, j>, <u, v>>, where color(u)=i and color(v)=j.
        input
            .mapToPair(
                pair -> {
                  Integer u = pair._1;
                  Integer v = pair._2;
                  Integer uColor = u % numColors;
                  Integer vColor = v % numColors;
                  return new Tuple2<>(new Tuple2<>(uColor, vColor), new Tuple2<>(u, v));
                })
            // For each key <i, j>, merge its values into a single |TIntArrayList|.
            //
            // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
            // TLDR: |groupByKey| will trigger the shuffling stage before the group-by-key action.
            // This
            // can cause huge performance overhead as each partition is talking to the rest to
            // exchange information. |aggregateByKey| on the other hand, performs the aggregation
            // within
            // the partition first, then shuffles. By replacing |groupByKey| with |aggregateByKey|,
            // the partition time on Arabic-2005 is reduced from 450s to 60s.
            .aggregateByKey(
                new TIntArrayList() /* initial value */,
                (list, tuple) -> {
                  /* what to do with the values from the same partition */
                  // since the edge is always represented by a pair of (two) nodes, then for a list
                  // {1, 5, 2, 7 ...}, we know the number of elements (nodes) must be even, and the
                  // edges must be 1->5, 2->7 etc.
                  list.add(tuple._1);
                  list.add(tuple._2);
                  return list;
                },
                (list1, list2) -> {
                  /* what to do with the values across the partitions */
                  list1.addAll(list2);
                  return list1;
                })
            .mapToPair(
                pair -> {
                  IntTIntArrayListHashMap neighborHashMap = new IntTIntArrayListHashMap();
                  TIntIterator it = pair._2.iterator();
                  while (it.hasNext()) {
                    // read in a pair of nodes.
                    int u = it.next();
                    int v = it.next();

                    if (neighborHashMap.get(u) == null) {
                      neighborHashMap.put(u, new TIntArrayList());
                    }
                    neighborHashMap.get(u).add(v);
                  }
                  // Sort is necessary. when tuple< <i, j>, [...] > is emitted, there is no
                  // guaranteed
                  // ordering on the nodes list.
                  // If we have <(0, 1), (3, 1)> and <(0, 1), (3, 4)> after first mapToPair, there
                  // is
                  // no guarantee if we will get <(0, 1), [3, 1, 3, 4]> or <(0, 1), [3, 4, 3, 1]>
                  // after
                  // aggregateByKey.
                  neighborHashMap.forEachValue(
                      (arr) -> {
                        arr.sort();
                        return true;
                      });
                  return new Tuple2<>(pair._1, neighborHashMap);
                });
    partitionMap.saveAsHadoopFile(fullPath, String.class, String.class, EdgesetOutputFormat.class);
  }

  /**
   * If we sort and relabel the vertices based on a function F, the graph (and its transpose) will
   * not change in terms of the number of triangles, but we can ensure the largest degree is
   * O(|E|^1.5).
   *
   * <p>F is defined as: F(u) < F(v) iff du < dv OR (du == dv AND u < v).
   *
   * <p>This version applies preprocessing to the original and the transpose at the same time.
   *
   * <p>Only the "cut" portion of the original implementation is parallelized using Spark, as the
   * "cut" operates on the adjacency list at O(V+E), which is quadratic. The sort is done on the
   * Spark driver to avoid the overhead using Spark.
   *
   * @param original Original graph.
   * @param transpose Graph transpose.
   * @return The 2-tuple of preprocessed graph and its transpose.
   */
  public static Tuple2<JavaPairRDD<Integer, int[]>, JavaPairRDD<Integer, int[]>> preprocessDual(
      JavaPairRDD<IntWritable, IntArrayWritable> original,
      JavaPairRDD<IntWritable, IntArrayWritable> transpose) {

    // first, calculate the total number of nodes, and max vertex degree (from both original and
    // transpose).

    // accumulate |N| and max degree for the original and transpose.
    Function2<
            Tuple2<Integer, Integer> /* accumulator */,
            Tuple2<IntWritable, IntArrayWritable> /* RDD pair */,
            Tuple2<Integer, Integer> /* ret val */>
        f1 = (acc, pair) -> new Tuple2<>(acc._1 + 1, Math.max(acc._2, pair._2.get().length));
    Function2<
            Tuple2<Integer, Integer> /* accumulator1 */,
            Tuple2<Integer, Integer> /* accumulator2 */,
            Tuple2<Integer, Integer> /* ret val */>
        f2 = (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, Math.max(acc1._2, acc2._2));
    // <# of vtx, max vtx degree>
    Tuple2<Integer, Integer> gInfo =
        original.aggregate(
            new Tuple2<>(0, 0) /* initial value */,
            f1 /* accumulate on the initial value with the values from the same partition */,
            f2 /* combine the accumulators across the partitions */);
    Tuple2<Integer, Integer> gtInfo =
        transpose.aggregate(
            new Tuple2<>(0, 0) /* initial value */,
            f1 /* accumulate on the initial value with the values from the same partition */,
            f2 /* combine the accumulators across the partitions */);
    int N = gInfo._1;
    assert (N == gtInfo._1);

    // second, populate |degree| and |new2Old| array.

    // <degree, new2Old>
    TIntIntHashMap degree;

    // accumulate |degree| and |new2Old|.
    Function2<
            TIntIntHashMap /* accumulator */,
            Tuple2<IntWritable, IntArrayWritable> /* RDD pair */,
            TIntIntHashMap /* ret val */>
        f3 =
            (acc, pair) -> {
              int u = pair._1.get();
              // degree
              assert acc.get(u) == acc.getNoEntryValue();
              acc.put(u, pair._2.get().length);
              return acc;
            };
    Function2<
            TIntIntHashMap /* accumulator1 */,
            TIntIntHashMap /* accumulator2 */,
            TIntIntHashMap /* ret val */>
        f4 =
            (acc1, acc2) -> {
              acc1.putAll(acc2);
              return acc1;
            };
    // aggregate original graph or transpose to populate |degree| and |new2Old|.
    if (gInfo._2 > gtInfo._2) {
      degree =
          original.aggregate(
              new TIntIntHashMap() /* initial value */,
              f3 /* accumulate on the initial value with the values from the same partition */,
              f4 /* combine the accumulators across the partitions */);
    } else {
      degree =
          transpose.aggregate(
              new TIntIntHashMap() /* initial value */,
              f3 /* accumulate on the initial value with the values from the same partition */,
              f4 /* combine the accumulators across the partitions */);
    }

    int[] new2Old = new int[N];
    for (int i = 0; i < N; ++i) new2Old[i] = i;
    // Sort |new2Old| using |degree|. Executed on the driver. The Spark's |sortBy| spends most of
    // time on task serialization, hence ineffective.

    // must use a *stable* sort.
    //
    // assume
    // new2Old: [5 3 6 0 1 2 4]
    // idx:      0 1 2 3 4 5 6
    // meaning the original vertex labelled 5 is now vtx 0, original 3 is 1 etc.
    IntArrays.mergeSort(
        new2Old,
        new IntComparator() {
          @Override
          public int compare(int u, int v) {
            return degree.get(u) - degree.get(v);
          }

          @Override
          public int compare(Integer u, Integer v) {
            return degree.get(u) - degree.get(v);
          }
        });

    // Invert |new2Old| to |old2New|. Executed on driver.
    int[] old2New = new int[N];
    for (int i = 0; i < N; ++i) {
      old2New[new2Old[i]] = i;
    }

    // Last, apply sort-and-cut to original and transpose.
    PairFunction<Tuple2<IntWritable, IntArrayWritable>, Integer, int[]> f5 =
        tuple2 -> {
          int oldId = tuple2._1.get();
          int newId = old2New[oldId];
          int[] oldSuccessors = tuple2._2.getValues();
          int len = 0;
          int[] newSuccessors = new int[oldSuccessors.length];
          for (int oldSuccessor : oldSuccessors) {
            if (newId < old2New[oldSuccessor]) {
              newSuccessors[len++] = old2New[oldSuccessor];
            }
          }
          return new Tuple2<>(newId, Arrays.copyOf(newSuccessors, len));
        };
    JavaPairRDD<Integer, int[]> originalSortAndCut = original.mapToPair(f5);
    JavaPairRDD<Integer, int[]> transposeSortAndCut = transpose.mapToPair(f5);
    return new Tuple2<>(originalSortAndCut, transposeSortAndCut);
  }

  /**
   * Sort-relabel implementation applied to a single graph, where all neighbours are kept after
   * sorting.
   *
   * @param graph The undirected graph.
   * @return Preprocessed graph.
   */
  public static JavaPairRDD<Integer, int[]> preprocessSingle(
      JavaPairRDD<IntWritable, IntArrayWritable> graph) {

    // first, calculate the total number of nodes, and max vertex degree (from both original and
    // transpose).

    // accumulate |N| for the undirected graph.
    Integer N =
        graph.aggregate(
            0 /* initial value */,
            (acc, pair) ->
                acc
                    + 1 /* accumulate on the initial value with the values from the same partition */,
            Integer::sum /* combine the accumulators across the partitions */);

    // second, populate |degree| and |new2Old| array.

    // degree
    TIntIntHashMap degree =
        graph.aggregate(
            new TIntIntHashMap() /* initial value */,
            (acc, pair) -> {
              int u = pair._1.get();
              // degree
              assert acc.get(u) == acc.getNoEntryValue();
              acc.put(u, pair._2.get().length);
              return acc;
            } /* accumulate on the initial value with the values from the same partition */,
            (acc1, acc2) -> {
              acc1.putAll(acc2);
              return acc1;
            } /* combine the accumulators across the partitions */);

    int[] new2Old = new int[N];
    for (int i = 0; i < N; ++i) new2Old[i] = i;
    // Sort |new2Old| using |degree|. Executed on the driver. The Spark's |sortBy| spends most of
    // time on task serialization, hence ineffective.

    // must use a *stable* sort.
    //
    // assume
    // new2Old: [5 3 6 0 1 2 4]
    // idx:      0 1 2 3 4 5 6
    // meaning the original vertex labelled 5 is now vtx 0, original 3 is 1 etc.
    IntArrays.mergeSort(
        new2Old,
        new IntComparator() {
          @Override
          public int compare(int u, int v) {
            return degree.get(u) - degree.get(v);
          }

          @Override
          public int compare(Integer u, Integer v) {
            return degree.get(u) - degree.get(v);
          }
        });

    // Invert |new2Old| to |old2New|. Executed on driver.
    int[] old2New = new int[N];
    for (int i = 0; i < N; ++i) {
      old2New[new2Old[i]] = i;
    }

    // Last, apply sort-and-cut to original and transpose.
    return graph.mapToPair(
        tuple2 -> {
          int oldId = tuple2._1.get();
          int newId = old2New[oldId];
          int[] oldSuccessors = tuple2._2.getValues();
          int len = 0;
          int[] newSuccessors = new int[oldSuccessors.length];
          for (int oldSuccessor : oldSuccessors) {
            newSuccessors[len++] = old2New[oldSuccessor];
          }
          return new Tuple2<>(newId, Arrays.copyOf(newSuccessors, len));
        });
  }

  /**
   * C++'s std::upper_bound. Returns the the lowest index where the array value is strictly greater
   * than |key|.
   */
  public static int upperBound(int[] arr, int key) {
    int lo = 0, hi = arr.length;
    while (lo < hi) {
      int mid = lo + (hi - lo) / 2;
      if (arr[mid] <= key) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}

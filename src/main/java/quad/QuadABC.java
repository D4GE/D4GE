package quad;

import commons.EnumerationABC;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SerializableWritable;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple4;

public abstract class QuadABC extends EnumerationABC {

  protected static final String symmPath = "edgesets_symm/";

  public QuadABC(String[] args) throws URISyntaxException, IOException {
    super(args);
  }

  @Override
  protected void run() throws Exception {
    generateSeed();

    double partitionTime = System.currentTimeMillis();
    if (preProc) {
      preprocessAndPartition();
    } else {
      partitionGraph();
    }
    partitionTime = (System.currentTimeMillis() - partitionTime) / 1000.0;

    double enumTime = System.currentTimeMillis();
    // broadcast the Hadoop config for future use in the enumeration stage. In the enumeration
    // stage, the executor will need to access the color(partition) files over the HDFS.
    Broadcast<SerializableWritable<Configuration>> broadcastHadoopConf =
        jsc.broadcast(new SerializableWritable<>(jsc.hadoopConfiguration()));
    Tuple4<QuadGraphletAccumulator, Long, List<Double>, List<Double>> res =
        enumerateQuad(broadcastHadoopConf);
    QuadGraphletAccumulator cnt = res._1();
    long kiloBytes = res._2();
    double slowestEnumTime = Collections.max(res._3());
    Median med = new Median();
    double medianEnumTime = med.evaluate(res._3().stream().mapToDouble(d -> d).toArray());
    double medianReadTime = med.evaluate(res._4().stream().mapToDouble(d -> d).toArray());

    enumTime = (System.currentTimeMillis() - enumTime) / 1000.0;

    FSDataOutputStream outputStream =
        fileSystem.create(new Path(inputDir + "/" + resultName), true);
    BufferedWriter resultWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
    String fmt =
        String.format(
            "%s %s preprocessing, querying %s",
            originalGraphName, preProc ? "with" : "without", queryGraph);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt = String.format("Number of colors: %d", numColors);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt = String.format("Partition time: %.4f sec", partitionTime);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt = String.format("Enumeration time (unweighted total, including read): %.4f sec", enumTime);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt =
        String.format(
            "Slowest enumeration time of all reducers (excluding read): %.4f sec", slowestEnumTime);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt =
        String.format(
            "Median enumeration time of all reducers (excluding read): %.4f sec", medianEnumTime);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt =
        String.format(
            "Median network read time of all reducers (excluding read): %.4f sec", medianReadTime);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt = String.format("Network read: %.4f MB", kiloBytes / 1024.0);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    fmt = String.format("%s", cnt);
    resultWriter.write(fmt);
    resultWriter.newLine();
    System.out.println(fmt);

    resultWriter.close();
    jsc.stop();
  }

  /** CD_ext and PTE generate different seed file. */
  protected abstract void generateSeed() throws IOException;

  /**
   * Use sort-n-cut to preprocess the graph input, and partition it. The pre-processing is
   * parallelized using Spark.
   */
  protected abstract void preprocessAndPartition() throws IOException;

  /**
   * Returns <4-node graphlet counts, kilobyte read, slowest reducer enum timer>
   *
   * @param hadoopConf Broadcast Hadoop configuration used to restore FileSystem in each worker.
   */
  public abstract Tuple4<QuadGraphletAccumulator, Long, List<Double>, List<Double>> enumerateQuad(
      Broadcast<SerializableWritable<Configuration>> hadoopConf) throws IOException;
}

package commons.map;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;
import scala.Tuple2;

/** This class is used to write out |Tuple2<Integer, Integer>, TIntIntArrayHashMap|. */
public class EdgesetOutputFormat
    extends MultipleTextOutputFormat<Tuple2<Integer, Integer>, IntTIntArrayListHashMap> {

  public static final String outputFormat = "%d_%d.edgeset";

  @Override
  public RecordWriter<Tuple2<Integer, Integer>, IntTIntArrayListHashMap> getRecordWriter(
      FileSystem fs, JobConf job, String leaf, Progressable arg3) {

    final String myName = generateLeafFileName(leaf);

    return new RecordWriter<Tuple2<Integer, Integer>, IntTIntArrayListHashMap>() {

      public void write(Tuple2<Integer, Integer> key, IntTIntArrayListHashMap value)
          throws IOException {

        // get the file name based on the key
        String keyBasedPath = generateFileNameForKeyValue(key, value, myName);
        // get the actual value
        IntTIntArrayListHashMap actualValue = generateActualValue(key, value);

        Path path = MultipleTextOutputFormat.getOutputPath(job);
        path = new Path(path, keyBasedPath);

        try (FSDataOutputStream output = fs.create(path, arg3)) {
          actualValue.out(output);
        }
      }

      public void close(Reporter reporter) {}
    };
  }

  @Override
  protected String generateFileNameForKeyValue(
      Tuple2<Integer, Integer> key, IntTIntArrayListHashMap value, String name) {
    return String.format(outputFormat, key._1, key._2);
  }
}

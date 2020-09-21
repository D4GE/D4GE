package quad;

import java.io.Serializable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class QuadGraphletAccumulator implements Serializable {

  private final LongAccumulator[] cnts = new LongAccumulator[9];
  private final LongAccumulator[] dups = new LongAccumulator[9];

  public QuadGraphletAccumulator(JavaSparkContext jsc) {
    for (int i = 1; i <= 8; ++i) {
      cnts[i] = jsc.sc().longAccumulator();
      dups[i] = jsc.sc().longAccumulator();
    }
  }

  public void accumulateCounts(Long... vals) {
    assert (vals.length == 9);
    for (int i = 1; i <= 8; ++i) {
      cnts[i].add(vals[i]);
    }
  }

  public void accumulateCounts(long... vals) {
    assert (vals.length == 9);
    for (int i = 1; i <= 8; ++i) {
      cnts[i].add(vals[i]);
    }
  }

  public void accumulateDups(Long... vals) {
    assert (vals.length == 9);
    for (int i = 1; i <= 8; ++i) {
      dups[i].add(vals[i]);
    }
  }

  public void accumulateDups(long... vals) {
    assert (vals.length == 9);
    for (int i = 1; i <= 8; ++i) {
      dups[i].add(vals[i]);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Induced subgraphs:\n");
    String[] pfx =
        new String[] {
            "",
            "g1(wedges)",
            "g2(triangles)",
            "g3(3-path)",
            "g4(3-star)",
            "g5(square)",
            "g6(lollipop)",
            "g7(diamond)",
            "g8(k4)"
        };
    for (int i = 1; i < 8; ++i) sb.append(String.format("%s: %,d\n", pfx[i], cnts[i].value()));
    sb.append(String.format("%s: %,d\n", pfx[8], cnts[8].value()));

    sb.append("\nduplicates:\n");
    for (int i = 1; i < 8; ++i) sb.append(String.format("%s: %,d\n", pfx[i], dups[i].value()));
    sb.append(String.format("%s: %,d\n", pfx[8], dups[8].value()));

    long threePath = cnts[3].value(),
        threeStar = cnts[4].value(),
        square = cnts[5].value(),
        lollipop = cnts[6].value(),
        diamond = cnts[7].value(),
        k4 = cnts[8].value();
    sb.append("\nsubgraphs:\n");
    sb.append(
        String.format(
            "3path: %,d\n", 12 * k4 + 6 * diamond + 2 * lollipop + 4 * square + threePath));
    sb.append(String.format("3star: %,d\n", 4 * k4 + 2 * diamond + lollipop + threeStar));
    sb.append(String.format("square: %,d\n", 3 * k4 + diamond + square));
    sb.append(String.format("lollipop: %,d\n", 12 * k4 + 4 * diamond + lollipop));
    sb.append(String.format("diamond: %,d\n", 6 * k4 + diamond));
    sb.append(String.format("k4: %,d\n", k4));

    return sb.toString();
  }
}

package quad;

import commons.map.IntIntArrayHashMap;
import scala.Tuple2;

public class K4Enumerator implements IEnumerator {
  private final IntIntArrayHashMap Ggt;
  private final long[] encodedProblems;
  private final int[] groupColors;
  private final int numColors;
  public final Long[] counts = {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L}; // g3 to g8.
  public final Long[] dups = {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L};

  public K4Enumerator(IntIntArrayHashMap Ggt, int[] groupColors, long[] encodedProblems, int numColors) {
    this.Ggt = Ggt;
    this.groupColors = groupColors;
    this.encodedProblems = encodedProblems;
    this.numColors = numColors;
  }

  public Tuple2<Long[], Long[]> countQuadGraphlet() {
    Ggt.forEach(
        (int u) -> {
          int[] uN = getGtNeighbours(u);
          for (int v : uN) {
            assert u < v;
            int[] vN = getGtNeighbours(v);
            for (int i = 0, j = 0; i < uN.length && j < vN.length; ) {
              int uPrime = uN[i], vPrime = vN[j];
              assert uPrime > u && vPrime > v;
              // Increment the indices.
              if (uPrime == vPrime) {
                ++i;
                ++j;
              } else if (uPrime < vPrime) {
                ++i;
              } else {
                ++j;
              }
              // Triangle found, with u < v < u'.
              if (uPrime == vPrime) {
                exploreTriangle(u, v, uPrime, uN, vN, getGtNeighbours(uPrime));
              }
            }
          }
          return true;
        });
    return new Tuple2<>(counts, dups);
  }

  /** Explore g8, g7 and g6 based off triangle(u, v, w) with u < v < w. */
  private void exploreTriangle(int u, int v, int w, int[] uN, int[] vN, int[] wN) {
    for (int i = 0, j = 0, k = 0; i < uN.length && j < vN.length && k < wN.length; ) {
      int uPrime = uN[i];
      int vPrime = vN[j];
      int wPrime = wN[k];

      // 4-clique / g8
      if (uPrime == vPrime && vPrime == wPrime) {
        incrementCounter(u, v, w, uPrime, 8);
        ++i;
        ++j;
        ++k;
      }
      // diamond / g7; w-z are opposite vertices.
      else if (uPrime == vPrime && vPrime < wPrime) {
        ++i;
        ++j;
      }
      // diamond / g7; v-z are opposite vertices.
      else if (uPrime == wPrime && wPrime < vPrime) {
        ++i;
        ++k;
      }
      // diamond / g7; u-z are opposite vertices.
      else if (vPrime == wPrime && wPrime < uPrime) {
        ++j;
        ++k;
      }
      // tailed triangle at u
      else if (uPrime < vPrime && uPrime < wPrime) {
        ++i;
      }
      // tailed triangle at v
      else if (vPrime < uPrime && vPrime < wPrime) {
        ++j;
      }
      // tailed triangle at w
      else if (wPrime < uPrime && wPrime < vPrime) {
        ++k;
      }
    }
  }

  private void incrementCounter(int u, int v, int w, int z, int graphletType) {
    int uColor = u % numColors,
        vColor = v % numColors,
        wColor = w % numColors,
        zColor = z % numColors;
    long p = 0L;
    for (int i = 0; i < groupColors.length; ++i) {
      if (groupColors[i] == uColor) p |= 1 << i;
      if (groupColors[i] == vColor) p |= 1 << i;
      if (groupColors[i] == wColor) p |= 1 << i;
      if (groupColors[i] == zColor) p |= 1 << i;
    }
    for (long problem : encodedProblems) {
      if (p == problem) {
        ++counts[graphletType];
      } else {
        ++dups[graphletType];
      }
    }
  }

  private int[] getGtNeighbours(int u) {
    int[] uN = Ggt.get(u);
    if (uN == null) uN = new int[0];
    return uN;
  }
}

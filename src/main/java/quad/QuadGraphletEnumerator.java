package quad;

import commons.Commons;
import commons.map.IntIntArrayHashMap;
import java.util.Arrays;
import scala.Tuple2;

public class QuadGraphletEnumerator implements IEnumerator {

  private final IntIntArrayHashMap Gsymm;
  private final long[] encodedProblems;
  private final int[] groupColors;
  private final int numColors;
  public final Long[] counts = {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L}; // g3 to g8.
  public final Long[] dups = {0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L};
  private final boolean[] explore = new boolean[9];

  public QuadGraphletEnumerator(
      IntIntArrayHashMap gsymm,
      int[] groupColors,
      long[] encodedProblems,
      int numColors,
      String queryGraph) {
    this.Gsymm = gsymm;
    this.groupColors = groupColors;
    this.encodedProblems = encodedProblems;
    this.numColors = numColors;
    if (queryGraph.equals("all")) Arrays.fill(explore, true);
    for (int i = 1; i <= 8; ++i) {
      if (queryGraph.equals(String.format("g%d", i))) {
        explore[i] = true;
      }
    }
  }

  public Tuple2<Long[], Long[]> countQuadGraphlet() {
    Gsymm.forEach(
        (int u) -> {
          int[] uNeighboursAll = getAllNeighbours(u);
          int uStart = Commons.upperBound(uNeighboursAll, u);
          for (int vIdx = uStart; vIdx < uNeighboursAll.length; ++vIdx) {
            int v = uNeighboursAll[vIdx];
            // u must < v.
            if (u >= v) {
              continue;
            }
            int[] vNeighboursAll = getAllNeighbours(v);
            // DON'T skip if j==vNeighboursAll.length. For wedge finding, even though v does not
            // have any neighbours greater than u, we still need to check u's neighbours in the
            // upcoming loop.
            int j = Commons.upperBound(vNeighboursAll, u);
            int i = uStart;
            while (i < uNeighboursAll.length || j < vNeighboursAll.length) {
              int uPrime = i < uNeighboursAll.length ? uNeighboursAll[i] : Integer.MAX_VALUE;
              int vPrime = j < vNeighboursAll.length ? vNeighboursAll[j] : Integer.MAX_VALUE;
              // Increment the indices.
              if (uPrime == vPrime && uPrime != Integer.MAX_VALUE) {
                ++i;
                ++j;
              } else if (uPrime < vPrime) {
                ++i;
              } else {
                ++j;
              }
              assert uPrime > u && vPrime > u;
              // Triangle found, with u < v < u'.
              if (uPrime == vPrime && uPrime > v && uPrime != Integer.MAX_VALUE) {
                if (explore[2]) incrementCounter(u, v, uPrime, u, 2);
                if (explore[8] || explore[7] || explore[6]) {
                  int[] wNeighboursAll = getAllNeighbours(uPrime);
                  exploreTriangle(u, v, uPrime, uNeighboursAll, vNeighboursAll, wNeighboursAll);
                }
              }
              // Wedge1 v-u-uPrime. u is the smallest vtx in the middle. Only list the ones with
              // u<v<uPrime, since while u is the middle vtx, uPrime-u-v is symmetrical to
              // v-u-uPrime.
              else if (v < uPrime && uPrime < vPrime) {
                if (explore[1]) incrementCounter(u, v, uPrime, u, 1);
                if (explore[5] || explore[4] || explore[3]) {
                  int[] wNeighboursAll = getAllNeighbours(uPrime);
                  exploreWedgeType1(
                      u, v, uPrime, uNeighboursAll, uStart, vNeighboursAll, wNeighboursAll);
                }
              }
              // Wedge2 u-v-vPrime. u is the smallest vtx on the leg. There is NO guarantee that
              // vPrime>v.
              else if (uPrime > vPrime) {
                if (explore[1]) incrementCounter(u, v, vPrime, u, 1);
                if (explore[4] || explore[3]) {
                  int[] wNeighboursAll = getAllNeighbours(vPrime);
                  exploreWedgeType2(
                      u, v, vPrime, uNeighboursAll, uStart, vNeighboursAll, wNeighboursAll);
                }
              }
            }
          }
          return true;
        });
    return new Tuple2<>(counts, dups);
  }

  /** Explore g8, g7 and g6 based off triangle(u, v, w) with u < v < w. */
  private void exploreTriangle(
      int u, int v, int w, int[] uNeighboursAll, int[] vNeighboursAll, int[] wNeighboursAll) {
    for (int i = 0, j = 0, k = 0;
        i < uNeighboursAll.length || j < vNeighboursAll.length || k < wNeighboursAll.length; ) {
      int uPrime = i < uNeighboursAll.length ? uNeighboursAll[i] : Integer.MAX_VALUE;
      int vPrime = j < vNeighboursAll.length ? vNeighboursAll[j] : Integer.MAX_VALUE;
      int wPrime = k < wNeighboursAll.length ? wNeighboursAll[k] : Integer.MAX_VALUE;

      // 4-clique / g8
      if (uPrime == vPrime && vPrime == wPrime && wPrime != Integer.MAX_VALUE) {
        if (uPrime > w && explore[8]) {
          incrementCounter(u, v, w, uPrime, 8);
        }
        ++i;
        ++j;
        ++k;
      }
      // diamond / g7; w-z are opposite vertices.
      else if (uPrime == vPrime && vPrime < wPrime) {
        if (vPrime > w && explore[7]) {
          incrementCounter(u, v, w, vPrime, 7);
        }
        ++i;
        ++j;
      }
      // diamond / g7; v-z are opposite vertices.
      else if (uPrime == wPrime && wPrime < vPrime) {
        if (wPrime > v && explore[7]) {
          incrementCounter(u, v, w, wPrime, 7);
        }
        ++i;
        ++k;
      }
      // diamond / g7; u-z are opposite vertices.
      else if (vPrime == wPrime && wPrime < uPrime) {
        if (wPrime > u && explore[7]) {
          incrementCounter(u, v, w, wPrime, 7);
        }
        ++j;
        ++k;
      }
      // tailed triangle at u
      else if (uPrime < vPrime && uPrime < wPrime) {
        if (explore[6]) incrementCounter(u, v, w, uPrime, 6);
        ++i;
      }
      // tailed triangle at v
      else if (vPrime < uPrime && vPrime < wPrime) {
        if (explore[6]) incrementCounter(u, v, w, vPrime, 6);
        ++j;
      }
      // tailed triangle at w
      else if (wPrime < uPrime && wPrime < vPrime) {
        if (explore[6]) incrementCounter(u, v, w, wPrime, 6);
        ++k;
      }
    }
  }

  /** Explore g5, g4 and g3 based off wedge(v, u, w) with u < v < w. */
  private void exploreWedgeType1(
      int u,
      int v,
      int w,
      int[] uNeighboursGt,
      int uStart,
      int[] vNeighboursAll,
      int[] wNeighboursAll) {
    int vStart = Commons.upperBound(vNeighboursAll, u);
    int wStart = Commons.upperBound(wNeighboursAll, u);
    for (int i = uStart, j = vStart, k = wStart;
        i < uNeighboursGt.length || j < vNeighboursAll.length || k < wNeighboursAll.length; ) {
      int uPrime = i < uNeighboursGt.length ? uNeighboursGt[i] : Integer.MAX_VALUE;
      int vPrime = j < vNeighboursAll.length ? vNeighboursAll[j] : Integer.MAX_VALUE;
      int wPrime = k < wNeighboursAll.length ? wNeighboursAll[k] : Integer.MAX_VALUE;

      // A diamond; should be found through triangle.
      if (uPrime == vPrime && vPrime == wPrime && wPrime != Integer.MAX_VALUE) {
        ++i;
        ++j;
        ++k;
      }
      // A tailed triangle with tail u-w; should be found through triangle.
      else if (uPrime == vPrime && vPrime < wPrime) {
        ++i;
        ++j;
      }
      // A tailed triangle with tail u-v; should be found through triangle.
      else if (uPrime == wPrime && wPrime < vPrime) {
        ++i;
        ++k;
      }
      // A rectangle / g5;
      else if (vPrime == wPrime && wPrime < uPrime) {
        if (vPrime > u && explore[5]) {
          incrementCounter(u, v, w, vPrime, 5);
        }
        ++j;
        ++k;
      }
      // A 3-start / g4.
      //     uPrime
      //       |
      //     v-u-w
      else if (uPrime < vPrime && uPrime < wPrime) {
        if (uPrime > w && explore[4]) {
          incrementCounter(u, v, w, uPrime, 4);
        }
        ++i;
      }
      // A 3-path vPrime-v-u-w / g3
      else if (vPrime < uPrime && vPrime < wPrime) {
        if (vPrime > u && explore[3]) {
          incrementCounter(u, v, w, vPrime, 3);
        }
        ++j;
      }
      // A 3-path v-u-w-wPrime / g3
      else if (wPrime < uPrime && wPrime < vPrime) {
        if (wPrime > u && explore[3]) {
          incrementCounter(u, v, w, wPrime, 3);
        }
        ++k;
      }
    }
  }

  /** Explore g4 and g3 based off wedge(u, v, w) with u < v && u < w. */
  private void exploreWedgeType2(
      int u,
      int v,
      int w,
      int[] uNeighboursGt,
      int uStart,
      int[] vNeighboursAll,
      int[] wNeighboursAll) {
    int vStart = Commons.upperBound(vNeighboursAll, u);
    int wStart = Commons.upperBound(wNeighboursAll, u);
    for (int i = uStart, j = vStart, k = wStart;
        i < uNeighboursGt.length || j < vNeighboursAll.length || k < wNeighboursAll.length; ) {
      int uPrime = i < uNeighboursGt.length ? uNeighboursGt[i] : Integer.MAX_VALUE;
      int vPrime = j < vNeighboursAll.length ? vNeighboursAll[j] : Integer.MAX_VALUE;
      int wPrime = k < wNeighboursAll.length ? wNeighboursAll[k] : Integer.MAX_VALUE;

      // A diamond; should be found through triangle.
      if (uPrime == vPrime && vPrime == wPrime && wPrime != Integer.MAX_VALUE) {
        ++i;
        ++j;
        ++k;
      }
      // A tailed triangle with tail v-w; should be found through triangle.
      else if (uPrime == vPrime && vPrime < wPrime) {
        ++i;
        ++j;
      }
      // A rectangle; should be found through triangle.
      else if (uPrime == wPrime && wPrime < vPrime) {
        ++i;
        ++k;
      }
      // A tailed triangle with tail v-u; should be found through triangle.
      else if (vPrime == wPrime && wPrime < uPrime) {
        ++j;
        ++k;
      }
      // A 3-path uPrime-u-v-w; should be found through wedge type1.
      // This is the same as (u'-u-v)-w, with the smallest vtx u at the center.
      else if (uPrime < vPrime && uPrime < wPrime) {
        ++i;
      }
      // A 3-start / g4.
      //     vPrime
      //       |
      //     u-v-w
      else if (vPrime < uPrime && vPrime < wPrime) {
        if (vPrime > u && vPrime > w && explore[4]) {
          incrementCounter(u, v, w, vPrime, 4);
        }
        ++j;
      }
      // A 3-path u-v-w-wPrime / g3
      else if (wPrime < uPrime && wPrime < vPrime) {
        if (wPrime > u && wPrime != v && explore[3]) {
          incrementCounter(u, v, w, wPrime, 3);
        }
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

  private int[] getAllNeighbours(int u) {
    int[] uN = Gsymm.get(u);
    if (uN == null) {
      uN = new int[0];
    }
    return uN;
  }
}

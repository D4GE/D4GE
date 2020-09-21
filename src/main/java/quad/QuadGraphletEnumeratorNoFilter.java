package quad;

import commons.Commons;
import commons.map.IntIntArrayHashMap;
import java.util.Arrays;
import java.util.Map;

public class QuadGraphletEnumeratorNoFilter implements IEnumeratorNoFilter {

  private final Map<String, IntIntArrayHashMap> Gsymm;
  private final String uv, uw, uz, vw, vz, wz;
  private final long[] counts = new long[9]; // g3 to g8.
  private final boolean[] explore = new boolean[9];

  public QuadGraphletEnumeratorNoFilter(
      Map<String, IntIntArrayHashMap> gsymm, int[] config, String queryGraph) {
    Gsymm = gsymm;
    uv = String.format("%d-%d", config[0], config[1]);
    uw = String.format("%d-%d", config[0], config[2]);
    uz = String.format("%d-%d", config[0], config[3]);
    vw = String.format("%d-%d", config[1], config[2]);
    vz = String.format("%d-%d", config[1], config[3]);
    wz = String.format("%d-%d", config[2], config[3]);
    if (queryGraph.equals("all")) Arrays.fill(explore, true);
    for (int i = 1; i <= 8; ++i) {
      if (queryGraph.equals(String.format("g%d", i))) {
        explore[i] = true;
      }
    }
  }

  public long[] countQuadGraphlet() {
    Gsymm.get(uv)
        .forEach(
            (int u) -> {
              int[] uvAll = Gsymm.get(uv).get(u) == null ? new int[0] : Gsymm.get(uv).get(u);
              int start = Commons.upperBound(uvAll, u);
              for (int vIdx = start; vIdx < uvAll.length; ++vIdx) {
                int v = uvAll[vIdx];
                if (u >= v) continue; // TODO: do we need this?

                // intersect uw and vw to find w
                int[] uwAll = Gsymm.get(uw).get(u) == null ? new int[0] : Gsymm.get(uw).get(u);
                int[] vwAll = Gsymm.get(vw).get(v) == null ? new int[0] : Gsymm.get(vw).get(v);
                int i = Commons.upperBound(uwAll, u);
                int j = Commons.upperBound(vwAll, u);
                while (i < uwAll.length || j < vwAll.length) {
                  int uPrime = i < uwAll.length ? uwAll[i] : Integer.MAX_VALUE;
                  int vPrime = j < vwAll.length ? vwAll[j] : Integer.MAX_VALUE;
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
                    if (explore[2]) ++counts[2];
                    if (explore[8] || explore[7] || explore[6]) exploreTriangle(u, v, uPrime);
                  }
                  // Wedge1 v-u-uPrime. u is the smallest vtx in the middle. Only list the ones with
                  // u<v<uPrime, since while u is the middle vtx, uPrime-u-v is symmetrical to
                  // v-u-uPrime.
                  else if (v < uPrime && uPrime < vPrime) {
                    if (explore[1]) ++counts[1];
                    if (explore[5] || explore[4] || explore[3]) exploreWedgeType1(u, v, uPrime);
                  }
                  // Wedge2 u-v-vPrime. u is the smallest vtx on the leg. There is NO guarantee that
                  // vPrime>v.
                  else if (uPrime > vPrime) {
                    if (explore[1]) ++counts[1];
                    if (explore[4] || explore[3]) exploreWedgeType2(u, v, vPrime);
                  }
                }
              }

              return true;
            });
    return counts;
  }

  private void incrementCounter(int u, int v, int w, int z, int type) {
    ++counts[type];
  }

  private void exploreTriangle(int u, int v, int w) {
    int[] uzAll = Gsymm.get(uz).get(u) == null ? new int[0] : Gsymm.get(uz).get(u);
    int[] vzAll = Gsymm.get(vz).get(v) == null ? new int[0] : Gsymm.get(vz).get(v);
    int[] wzAll = Gsymm.get(wz).get(w) == null ? new int[0] : Gsymm.get(wz).get(w);

    for (int i = 0, j = 0, k = 0; i < uzAll.length || j < vzAll.length || k < wzAll.length; ) {
      int uPrime = i < uzAll.length ? uzAll[i] : Integer.MAX_VALUE;
      int vPrime = j < vzAll.length ? vzAll[j] : Integer.MAX_VALUE;
      int wPrime = k < wzAll.length ? wzAll[k] : Integer.MAX_VALUE;

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

  private void exploreWedgeType1(int u, int v, int w) {
    int[] uzAll = Gsymm.get(uz).get(u) == null ? new int[0] : Gsymm.get(uz).get(u);
    int[] vzAll = Gsymm.get(vz).get(v) == null ? new int[0] : Gsymm.get(vz).get(v);
    int[] wzAll = Gsymm.get(wz).get(w) == null ? new int[0] : Gsymm.get(wz).get(w);
    int uStart = Commons.upperBound(uzAll, u);
    int vStart = Commons.upperBound(vzAll, u);
    int wStart = Commons.upperBound(wzAll, u);
    for (int i = uStart, j = vStart, k = wStart;
        i < uzAll.length || j < vzAll.length || k < wzAll.length; ) {
      int uPrime = i < uzAll.length ? uzAll[i] : Integer.MAX_VALUE;
      int vPrime = j < vzAll.length ? vzAll[j] : Integer.MAX_VALUE;
      int wPrime = k < wzAll.length ? wzAll[k] : Integer.MAX_VALUE;

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

  private void exploreWedgeType2(int u, int v, int w) {
    int[] uzAll = Gsymm.get(uz).get(u) == null ? new int[0] : Gsymm.get(uz).get(u);
    int[] vzAll = Gsymm.get(vz).get(v) == null ? new int[0] : Gsymm.get(vz).get(v);
    int[] wzAll = Gsymm.get(wz).get(w) == null ? new int[0] : Gsymm.get(wz).get(w);
    int uStart = Commons.upperBound(uzAll, u);
    int vStart = Commons.upperBound(vzAll, u);
    int wStart = Commons.upperBound(wzAll, u);
    for (int i = uStart, j = vStart, k = wStart;
        i < uzAll.length || j < vzAll.length || k < wzAll.length; ) {
      int uPrime = i < uzAll.length ? uzAll[i] : Integer.MAX_VALUE;
      int vPrime = j < vzAll.length ? vzAll[j] : Integer.MAX_VALUE;
      int wPrime = k < wzAll.length ? wzAll[k] : Integer.MAX_VALUE;

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
}

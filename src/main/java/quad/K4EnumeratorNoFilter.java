package quad;

import commons.map.IntIntArrayHashMap;
import java.util.Map;

public class K4EnumeratorNoFilter implements IEnumeratorNoFilter {
  private final Map<String, IntIntArrayHashMap> Ggt;
  private final String uv, uw, uz, vw, vz, wz;
  private final long[] counts = new long[9]; // g3 to g8.

  public K4EnumeratorNoFilter(Map<String, IntIntArrayHashMap> ggt, int[] config) {
    Ggt = ggt;
    uv = String.format("%d-%d", config[0], config[1]);
    uw = String.format("%d-%d", config[0], config[2]);
    uz = String.format("%d-%d", config[0], config[3]);
    vw = String.format("%d-%d", config[1], config[2]);
    vz = String.format("%d-%d", config[1], config[3]);
    wz = String.format("%d-%d", config[2], config[3]);
  }

  public long[] countQuadGraphlet() {
    Ggt.get(uv)
        .forEach(
            (int u) -> {
              int[] uvAll = Ggt.get(uv).get(u) == null ? new int[0] : Ggt.get(uv).get(u);
              for (int v : uvAll) {
                assert u < v;

                // intersect uw and vw to find w
                int[] uwAll = Ggt.get(uw).get(u) == null ? new int[0] : Ggt.get(uw).get(u);
                int[] vwAll = Ggt.get(vw).get(v) == null ? new int[0] : Ggt.get(vw).get(v);
                for (int i = 0, j = 0; i < uwAll.length && j < vwAll.length; ) {
                  int uPrime = uwAll[i];
                  int vPrime = vwAll[j];
                  // Increment the indices.
                  if (uPrime == vPrime) {
                    ++i;
                    ++j;
                  } else if (uPrime < vPrime) {
                    ++i;
                  } else {
                    ++j;
                  }
                  assert uPrime > u && vPrime > v;
                  // Triangle found, with u < v < u'.
                  if (uPrime == vPrime) {
                    ++counts[2];
                    exploreTriangle(u, v, uPrime);
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
    int[] uzAll = Ggt.get(uz).get(u) == null ? new int[0] : Ggt.get(uz).get(u);
    int[] vzAll = Ggt.get(vz).get(v) == null ? new int[0] : Ggt.get(vz).get(v);
    int[] wzAll = Ggt.get(wz).get(w) == null ? new int[0] : Ggt.get(wz).get(w);

    for (int i = 0, j = 0, k = 0; i < uzAll.length && j < vzAll.length && k < wzAll.length; ) {
      int uPrime = uzAll[i];
      int vPrime = vzAll[j];
      int wPrime = wzAll[k];

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
}

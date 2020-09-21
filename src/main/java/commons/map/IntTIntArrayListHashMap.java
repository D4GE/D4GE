package commons.map;

import com.esotericsoftware.kryo.io.Output;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.OutputStream;

public class IntTIntArrayListHashMap
    extends TIntObjectHashMap<TIntArrayList> {

  public IntTIntArrayListHashMap() {
  }

  public void out(OutputStream os) {
    Output out = new Output(os);
    out.writeFloat(this._autoCompactionFactor);
    out.writeInt(this._autoCompactRemovesRemaining);
    out.writeBoolean(this._autoCompactTemporaryDisable);
    out.writeInt(this._free);
    out.writeInt(this._maxSize);
    out.writeInt(this._size);
    out.writeFloat(this._loadFactor);
    out.writeInt(this._set.length);
    out.writeInts(this._set, true);
    out.writeBytes(this._states);
    Object[] arrObject = this._values;
    for (Object o : arrObject) {
      try {
        int[] arr = ((TIntArrayList) o).toArray();
        out.writeInt(arr.length, true);
        out.writeInts(arr, true);
      } catch (NullPointerException e) {
        out.writeInt(0, true);
      }
    }
    out.close();
  }
}
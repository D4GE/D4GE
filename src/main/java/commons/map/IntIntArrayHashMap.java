package commons.map;

import com.esotericsoftware.kryo.io.Input;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.InputStream;
import java.lang.reflect.Array;

public class IntIntArrayHashMap
    extends TIntObjectHashMap<int[]> {

  public IntIntArrayHashMap() {
  }

  /**
   * read in from |InputStream|.
   */
  public void in(InputStream is) {
    Input in = new Input(is);
    this._autoCompactionFactor = in.readFloat();
    this._autoCompactRemovesRemaining = in.readInt();
    this._autoCompactTemporaryDisable = in.readBoolean();
    this._free = in.readInt();
    this._maxSize = in.readInt();
    this._size = in.readInt();
    this._loadFactor = in.readFloat();
    int length = in.readInt();
    this._set = in.readInts(length, true);
    this._states = in.readBytes(length);
    int[][] values = (int[][]) Array.newInstance(int[].class, length);
    for (int i = 0; i < length; ++i) {
      int vLength = in.readInt(true);
      values[i] = vLength == 0 ? null : in.readInts(vLength, true);
    }
    this._values = values;
    in.close();
  }
}

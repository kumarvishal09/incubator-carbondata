package org.apache.carbondata.core.datastore.columnar;

import java.util.Arrays;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class PrimitivePageIndexGenerator extends PageIndexGenerator<Object[]> {

  private Object[] dataPage;

  public PrimitivePageIndexGenerator(Object[] dataPage, boolean isSortRequired,
      DataType dataType) {
    this.dataPage = dataPage;
    if (isSortRequired) {
      PrimitiveColumnDataVO[] dataWithRowId = createColumnWithRowId(dataPage);
      SerializableComparator comparator =
          org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataType);
      Arrays.sort(dataWithRowId, comparator);
      short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
      rleEncodeOnRowId(rowIds);
    } else {
      this.rowIdRlePage = new short[0];
      this.invertedIndex = new short[0];
    }
    this.dataRlePage = new short[0];
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private PrimitiveColumnDataVO[] createColumnWithRowId(Object[] dataPage) {
    PrimitiveColumnDataVO[] columnWithIndexs =
        new PrimitiveColumnDataVO[dataPage.length];
    for (short i = 0; i < columnWithIndexs.length; i++) {
      columnWithIndexs[i] = new PrimitiveColumnDataVO(dataPage[i], i);
    }
    return columnWithIndexs;
  }

  private short[] extractDataAndReturnRowId(PrimitiveColumnDataVO[] dataWithRowId,
      Object[] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      dataPage[i] = dataWithRowId[i].getColumn();
    }
    this.dataPage = dataPage;
    return indexes;
  }

  @Override public Object[] getDataPage() {
    return dataPage;
  }

  @Override public int[] getLength() {
    throw new UnsupportedOperationException("Operation not supported");
  }
}

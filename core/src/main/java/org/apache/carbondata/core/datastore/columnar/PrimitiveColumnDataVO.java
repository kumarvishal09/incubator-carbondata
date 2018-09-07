package org.apache.carbondata.core.datastore.columnar;

public class PrimitiveColumnDataVO {

  private Object column;
  private short index;

  PrimitiveColumnDataVO(Object column, short index) {
    this.column = column;
    this.index = index;
  }

  @Override public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PrimitiveColumnDataVO o = (PrimitiveColumnDataVO) obj;
    return column.equals(o.column) && getIndex() == o.getIndex();
  }

  @Override public int hashCode() {
    return getColumn().hashCode() + new Short(getIndex()).hashCode();
  }

  /**
   * @return the index
   */
  public short getIndex() {
    return index;
  }

  /**
   * @return the column
   */
  public Object getColumn() {
    return column;
  }
}

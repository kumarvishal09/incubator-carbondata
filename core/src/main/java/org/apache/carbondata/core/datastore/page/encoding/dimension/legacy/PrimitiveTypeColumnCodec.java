package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.columnar.PrimitivePageIndexGenerator;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.format.Encoding;

public class PrimitiveTypeColumnCodec extends IndexStorageCodec {

  private DataType dataType;

  private Encoding encoding;

  public PrimitiveTypeColumnCodec(boolean isSort, boolean isInvertedIndex, Compressor compressor,
      DataType dataType, Encoding encoding) {
    super(isSort, isInvertedIndex, compressor);
    this.dataType = dataType;
    this.encoding = encoding;
  }

  @Override public String getName() {
    return "PrimitiveTypeColumnCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, Object> parameter) {
    return new IndexStorageEncoder(false, null == parameter ?
        null :
        (parameter.get("keygenerator") == null ?
            null :
            (KeyGenerator) parameter.get("keygenerator")), encoding) {
      @Override
      protected void encodeIndexStorage(ColumnPage input) {
        ColumnPage actualPage =
            !input.isLocalDictGeneratedPage() ? input : input.getLocalDictPage();
        PageIndexGenerator<Object[]> pageIndexGenerator;
        Object[] data = actualPage.getPageBasedOnDataType();
        pageIndexGenerator =
            new PrimitivePageIndexGenerator(data, isSort, actualPage.getDataType());
        ColumnPage adaptivePage;
        TableSpec.MeasureSpec spec = TableSpec.MeasureSpec
            .newInstance(actualPage.getColumnSpec().getFieldName(), dataType);
        try {
          adaptivePage =
              ColumnPage.newPage(spec, dataType, actualPage.getPageSize());
        } catch (MemoryException e) {
          throw new RuntimeException(e);
        }
        Object[] dataPage = pageIndexGenerator.getDataPage();
        adaptivePage.setStatsCollector(PrimitivePageStatsCollector.newInstance(dataType));
        for (int i = 0; i < dataPage.length; i++) {
          adaptivePage.putData(i, dataPage[i]);
        }
        try {
          this.encodedColumnPage = DefaultEncodingFactory.getInstance()
              .createEncoder(spec, adaptivePage, null).encode(adaptivePage);
        } catch (IOException | MemoryException e) {
          throw new RuntimeException(e);
        }
        adaptivePage.freeMemory();
        super.compressedDataPage = encodedColumnPage.getEncodedData().array();
        super.pageIndexGenerator = pageIndexGenerator;
      }
    };
  }
}

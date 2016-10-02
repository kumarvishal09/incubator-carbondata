package org.apache.carbondata.core.reader;

import java.io.IOException;

import org.apache.carbondata.format.DataChunk;

import org.apache.thrift.TBase;

public class CarbonDataChunkReader {

  /**
   * It reads the metadata in FileFooter thrift object format.
   *
   * @return
   * @throws IOException
   */
  public static DataChunk readFooter(String filePath, long offset) throws IOException {
    ThriftReader thriftReader = openThriftReader(filePath);
    thriftReader.open();
    //Set the offset from where it should read
    thriftReader.setReadOffset(offset);
    DataChunk datachunk = (DataChunk) thriftReader.read();
    thriftReader.close();
    return datachunk;
  }

  /**
   * Open the thrift reader
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  private static ThriftReader openThriftReader(String filePath) throws IOException {

    ThriftReader thriftReader = new ThriftReader(filePath, new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new DataChunk();
      }
    });
    return thriftReader;
  }
}

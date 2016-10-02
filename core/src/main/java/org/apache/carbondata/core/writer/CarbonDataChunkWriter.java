package org.apache.carbondata.core.writer;

import java.io.IOException;

import org.apache.carbondata.format.DataChunk;

public class CarbonDataChunkWriter {

  private ThriftWriter thriftWriter;

  /**
   * open thrift writer for writing dictionary chunk/meta object
   */
  public void initalizeThriftWriter(String filePath) throws IOException {
    // create thrift writer instance
    thriftWriter = new ThriftWriter(filePath, true);
    // open the file stream
    thriftWriter.open();
  }

  /**
   * It writes FileFooter thrift format object to file.
   *
   * @param footer
   * @param currentPosition At where this metadata is going to be written.
   * @throws IOException
   */
  public void writeFooter(DataChunk datachunk) throws IOException {
    try {
      thriftWriter.write(datachunk);
    } catch (Exception e) {
      throw e;
    }
  }

  public void close() {
    if (null != thriftWriter) {
      thriftWriter.close();
    }
  }
}

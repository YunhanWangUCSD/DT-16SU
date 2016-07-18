package com.example.FileToJdbcApp;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

public class FileReader extends LineByLineFileInputOperator{

  /**
   * output in bytes matching CsvParser input type
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<byte[]> byteOutput  = new DefaultOutputPort<>();

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
    byte[] bytes = tuple.getBytes();
    byteOutput.emit(bytes);
  }
}

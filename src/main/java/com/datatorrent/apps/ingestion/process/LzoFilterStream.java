package com.datatorrent.apps.ingestion.process;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.datatorrent.malhar.lib.io.fs.FilterStreamContext;
import com.datatorrent.malhar.lib.io.fs.FilterStreamProvider;

public class LzoFilterStream
{

  public static class LzoFiltertreamContext extends FilterStreamContext.BaseFilterStreamContext<CompressionOutputStream>
  {
    public LzoFiltertreamContext(String compressionClassName, OutputStream outputStream) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
    {
      Constructor c = Class.forName(compressionClassName).getConstructor(OutputStream.class);
      this.filterStream = (CompressionOutputStream) c.newInstance(outputStream);
    }
  }

  public static class LzoFilterStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<CompressionOutputStream, OutputStream>
  {
    private String compressorClassName;
    private LzoFiltertreamContext streamContext;

    @Override
    public FilterStreamContext<CompressionOutputStream> createFilterStreamContext(OutputStream outputStream)
    {
      try {
        streamContext = new LzoFiltertreamContext(this.compressorClassName, outputStream);
        return streamContext;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void setCompressionClassName(String compressionClassName)
    {
      this.compressorClassName = compressionClassName;
    }
    
    public long getTimeTaken()
    {
      return streamContext.getFilterStream().getTimeTaken();
    }
  }
}

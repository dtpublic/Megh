package com.datatorrent.apps.ingestion.process;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.datatorrent.malhar.lib.io.fs.FilterStreamContext;
import com.datatorrent.malhar.lib.io.fs.FilterStreamProvider;

public class LzoFilterStream
{

  public static class LzoFiltertreamContext extends FilterStreamContext.BaseFilterStreamContext<LzoOutputStream>
  {
    public LzoFiltertreamContext(String compressionClassName, OutputStream outputStream) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException, IOException
    {
      Constructor c = Class.forName(compressionClassName).getConstructor(OutputStream.class);
      this.filterStream = (LzoOutputStream) c.newInstance(outputStream);
    }
  }

  public static class LzoFilterStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<LzoOutputStream, OutputStream>
  {
    private String compressorClassName;

    @Override
    public FilterStreamContext<LzoOutputStream> createFilterStreamContext(OutputStream outputStream)
    {
      try {
        return new LzoFiltertreamContext(this.compressorClassName, outputStream);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void setCompressionClassName(String compressionClassName)
    {
      this.compressorClassName = compressionClassName;
    }
  }

}

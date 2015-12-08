package com.datatorrent.modules.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.module.HDFSInputModule;
import com.datatorrent.module.io.fs.HDFSFileCopyModule;

@ApplicationAnnotation(name = "HDFSFileCopyModuleDemo")
public class HDFSFileCopyModuleDemo implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    HDFSInputModule inputModule = dag.addModule("hdfsInputModule", new HDFSInputModule());
    HDFSFileCopyModule outputModule = dag.addModule("HDFSFileCopyModule", new HDFSFileCopyModule());

    dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
    dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
        .setLocality(Locality.THREAD_LOCAL);
    dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(Locality.THREAD_LOCAL);

  }

  private static Logger LOG = LoggerFactory.getLogger(HDFSFileCopyModuleDemo.class);
}

package com.datatorrent.apps.ingestion;

import java.util.Arrays;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.apps.ingestion.io.BlockReader;
import com.datatorrent.apps.ingestion.io.BlockWriter;
import com.datatorrent.apps.ingestion.io.ReaderWriterPartitioner;
import com.datatorrent.apps.ingestion.io.input.IngestionFileSplitter;
import com.datatorrent.core.api.Context;
import com.datatorrent.core.api.StatsListener;
import com.datatorrent.malhar.lib.counters.BasicCounters;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * @author chandni
 */
@ApplicationAnnotation(name = "Ingestion-subapp")
public class SubApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    IngestionFileSplitter fileSplitter = dag.addOperator("FileSplitter", IngestionFileSplitter.class);
    dag.setAttribute(fileSplitter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    BlockReader blockReader = dag.addOperator("BlockReader", BlockReader.class);
    dag.setAttribute(blockReader, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());
    blockReader.setCollectStats(false);

    BlockWriter blockWriter = dag.addOperator("BlockWriter", BlockWriter.class);
    dag.setAttribute(blockWriter, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    ReaderWriterPartitioner readerWriterPartitioner = new ReaderWriterPartitioner();

    dag.setAttribute(blockReader, Context.OperatorContext.PARTITIONER, readerWriterPartitioner);

    dag.setAttribute(fileSplitter, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{readerWriterPartitioner}));
    dag.setAttribute(blockReader, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{readerWriterPartitioner}));
    dag.setAttribute(blockWriter, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{readerWriterPartitioner}));

    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);
    dag.addStream("BlockData", blockReader.messages, blockWriter.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("ProcessedBlockmetadata", blockReader.blocksMetadataOutput, blockWriter.blockMetadataInput).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("FileMetadata", fileSplitter.filesMetadataOutput, synchronizer.filesMetadataInput);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);
    dag.addStream("MergeTrigger", synchronizer.trigger, console.input);
  }
}

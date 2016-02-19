package com.datatorrent.app;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.app.operators.KinesisBytesInputOperator;
import com.datatorrent.app.operators.S3BytesFileOutputOperator;
import com.datatorrent.contrib.kinesis.AbstractKinesisInputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="KinesisToS3App")
public class Application implements StreamingApplication
{
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        KinesisBytesInputOperator inputOp = dag.addOperator("FromKinesis", new KinesisBytesInputOperator());
        inputOp.getConsumer().setRecordsLimit(600);
        inputOp.setStrategy(AbstractKinesisInputOperator.PartitionStrategy.MANY_TO_ONE.toString());

        S3BytesFileOutputOperator s3output = dag.addOperator("WriteToS3", new S3BytesFileOutputOperator());

        dag.addStream("KinesisTOConsole", inputOp.outputPort, s3output.input);
    }
}
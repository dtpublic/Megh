package com.datatorrent.app.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class HDFSOutputOperator extends BytesFileOutputOperator
{
    protected static final String recoveryPath = "S3TmpFiles";
    public transient DefaultOutputPort output = new DefaultOutputPort();

    public HDFSOutputOperator()
    {
        filePath = "";
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
        filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + recoveryPath;
        super.setup(context);
    }

    @Override
    protected void finalizeFile(String fileName) throws IOException
    {
    }

    protected void rotate(String fileName) throws IllegalArgumentException, IOException, ExecutionException
    {
        super.rotate(fileName);
        String partFile = getPartFileNamePri(fileName);
        String tmpFileName = getFileNameToTmpName().get(partFile);
        String srcPath = filePath + Path.SEPARATOR + tmpFileName;
        int offset = endOffsets.get(fileName).intValue();
        S3Reconciler.OutputMetaData metaData = new S3Reconciler.OutputMetaData(srcPath, partFile, offset);
        output.emit(metaData);
    }
}

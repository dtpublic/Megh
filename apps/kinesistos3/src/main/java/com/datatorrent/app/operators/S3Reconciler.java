package com.datatorrent.app.operators;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.esotericsoftware.kryo.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class S3Reconciler extends AbstractReconciler<S3Reconciler.OutputMetaData, S3Reconciler.OutputMetaData>
{
    private static final Logger logger = LoggerFactory.getLogger(S3Reconciler.class);
    private static final String TMP_EXTENSION = ".tmp";
    @NotNull
    private String accessKey;
    @NotNull
    private String secretKey;
    @NotNull
    private String bucketName;
    @NotNull
    private String directoryName;
    protected transient AmazonS3 s3client;
    protected transient FileSystem fs;
    protected transient String filePath;

    @Override
    public void setup(Context.OperatorContext context)
    {
        s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey,secretKey));
        filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + HDFSOutputOperator.recoveryPath;
        try {
            fs = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());
        } catch (IOException e) {
            logger.error("Unable to create FileSystem: {}", e.getMessage());
        }
        super.setup(context);
    }

    @Override
    protected void processTuple(S3Reconciler.OutputMetaData path)
    {
        enqueueForProcessing(path);
    }

    @Override
    protected void processCommittedData(S3Reconciler.OutputMetaData path)
    {
        try {
            FSDataInputStream fsinput = fs.open(new Path(path.getPath()));
            ObjectMetadata omd = new ObjectMetadata();
            omd.setContentLength(path.getSize());
            String keyName = directoryName + Path.SEPARATOR + path.getFileName() ;
            s3client.putObject(new PutObjectRequest(bucketName, keyName, fsinput, omd));
        } catch (IOException e) {
            logger.error("Unable to create Stream: {}", e.getMessage());
        }
    }

    @Override
    public void endWindow()
    {
        while (doneTuples.peek() != null) {
            S3Reconciler.OutputMetaData metaData = doneTuples.poll();
            committedTuples.remove(metaData);
            try {
                Path dest = new Path(metaData.getPath());
                //Deleting the intermediate files and when writing to tmp files
                // there can be vagrant tmp files which we have to clean
                FileStatus[] statuses = fs.listStatus(dest.getParent());
                for (FileStatus status : statuses) {
                    String statusName = status.getPath().getName();
                    if (statusName.endsWith(TMP_EXTENSION) && statusName.startsWith(metaData.getFileName())) {
                        //a tmp file has tmp extension always preceded by timestamp
                        String actualFileName = statusName.substring(0, statusName.lastIndexOf('.', statusName.lastIndexOf('.') - 1));
                        if (metaData.getFileName().equals(actualFileName)) {
                            logger.info("deleting stray file {}", statusName);
                            fs.delete(status.getPath(), true);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Unable to Delete a file: {}", metaData.getFileName());
            }
        }
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getDirectoryName() {
        return directoryName;
    }

    public void setDirectoryName(String directoryName) {
        this.directoryName = directoryName;
    }

    public static class OutputMetaData
    {
        private String path;
        private String fileName;
        private int size;

        public OutputMetaData()
        {

        }
        public OutputMetaData(String path, String fileName, int size)
        {
            this.path = path;
            this.fileName = fileName;
            this.size = size;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }
}

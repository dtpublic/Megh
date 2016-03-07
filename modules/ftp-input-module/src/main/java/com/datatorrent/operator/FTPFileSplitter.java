package com.datatorrent.operator;

import com.datatorrent.lib.io.input.ModuleFileSplitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * FTPFileSplitter extends from ModuleFileSplitter and serves the functionality
 * of splitting the ftp file and creates metadata describing the files and splits.
 */
public class FTPFileSplitter extends ModuleFileSplitter
{
    public FTPFileSplitter()
    {
        super();
        super.setScanner(new FTPScanner());
    }

    public static class FTPScanner extends Scanner
    {
        private String inputURI;
        @Override
        protected FileSystem getFSInstance() throws IOException
        {
            DTFTPFileSystem ftpSystem = new DTFTPFileSystem();
            ftpSystem.initialize(URI.create(inputURI), new Configuration());
            return ftpSystem;
        }

        /**
         * Returns the inputURI
         * @return
         */
        public String getInputURI()
        {
            return inputURI;
        }

        /**
         * Sets the inputURI which is in the form of
         * @param inputURI inputURI
         */
        public void setInputURI(String inputURI)
        {
            this.inputURI = inputURI;
        }
    }
}


/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.module;

import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.input.BlockReader;
import com.datatorrent.lib.io.input.ModuleFileSplitter;
import com.datatorrent.operator.FTPBlockReader;
import com.datatorrent.operator.FTPFileSplitter;
import org.apache.hadoop.conf.Configuration;

/**
 * FTPInputModule which extends from FSInputModule and serves the functionality of reading files
 * from FTP server and emits FileMetadata, BlockMetadata and the block bytes.&nbsp;
 * <p>
 * <br>
 * Properties:<br>
 * <b>host</b>:  <br>
 * <b>port</b>:  <br>
 * <b>userName</b>: <br>
 * <b>password</b>: <br>
 *
 */
public class FTPInputModule extends FSInputModule
{
    private String host;
    private int port;
    private String userName;
    private String password;

    private transient String inputURI;

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
        generateURIAndConcatToFiles();
        super.populateDAG(dag,configuration);
    }

    protected void generateURIAndConcatToFiles()
    {
        inputURI = FTPBlockReader.SCHEME + "://" + userName + ":" + password + "@" + host
                + ":" + port + "/";
        String uriFiles = "";
        String[] inputFiles = getFiles().split(",");
        for (int i = 0; i < inputFiles.length; i++) {
            uriFiles += inputURI + inputFiles[i];
            if (i != inputFiles.length - 1) {
                uriFiles += ",";
            }
        }
        setFiles(uriFiles);
    }

    @Override
    public ModuleFileSplitter getFileSplitter()
    {
        FTPFileSplitter fs = new FTPFileSplitter();
        FTPFileSplitter.FTPScanner scanner = (FTPFileSplitter.FTPScanner)fs.getScanner();
        scanner.setInputURI(inputURI);
        return fs;
    }

    @Override
    public BlockReader getBlockReader()
    {
        FTPBlockReader br = new FTPBlockReader();
        br.setInputUri(inputURI);
        return br;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

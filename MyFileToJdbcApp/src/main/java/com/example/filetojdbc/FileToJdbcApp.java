package com.example.filetojdbc;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by YunhanWang on 6/27/16.
 */

@ApplicationAnnotation(name = "MyFileToJdbcApp")
public class FileToJdbcApp implements StreamingApplication{

    @Override
    public void populateDAG(DAG dag, Configuration configuration) {

        FileReader fileReader = dag.addOperator("FileReader", FileReader.class);



        // configure operators
        fileReader.setDirectory("/tmp/SimpleFileIO/input-dir");


    }
}

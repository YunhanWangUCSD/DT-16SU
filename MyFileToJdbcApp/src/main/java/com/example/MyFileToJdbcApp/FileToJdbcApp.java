package com.example.MyFileToJdbcApp;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcPOJOOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.util.FieldInfo;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Created by YunhanWang on 6/27/16.
 */

@ApplicationAnnotation(name = "MyFileToJdbcApp")
public class FileToJdbcApp implements StreamingApplication{

    @Override
    public void populateDAG(DAG dag, Configuration configuration) {

        FileReader fileReader = dag.addOperator("FileReader", FileReader.class);
        LineParser lineParser = dag.addOperator("LineParser", LineParser.class);
        JdbcPOJOOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOOutputOperator.class);
//        ConsoleOutputOperator consoleOutputOperator = new ConsoleOutputOperator();
//        dag.addOperator("ConsoleOutput", consoleOutputOperator);

        // configure operators
        fileReader.setDirectory("/tmp/yunhan/fileToJdbc/input-dir");

        jdbcOutputOperator.setTablename("test_file_to_jdbc_table");
        jdbcOutputOperator.setFieldInfos(addFieldInfos());
        JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
        jdbcOutputOperator.setStore(outputStore);

        // add stream
//        dag.addStream("POJO's",csvParser.parsedOutput, jdbcOutputOperator.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("data", fileReader.output, lineParser.input);
        dag.addStream("POJO", lineParser.output, jdbcOutputOperator.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
//        dag.addStream("test", lineParser.output, consoleOutputOperator.input);

    }

    /**
     * This method can be modified to have field mappings based on used defined
     * class
     */
    private List<FieldInfo> addFieldInfos()
    {
        List<FieldInfo> fieldInfos = Lists.newArrayList();
        fieldInfos.add(new FieldInfo("ACCOUNT_NO", "accountNumber", FieldInfo.SupportType.INTEGER));
        fieldInfos.add(new FieldInfo("NAME", "name", FieldInfo.SupportType.STRING));
        fieldInfos.add(new FieldInfo("AMOUNT", "amount", FieldInfo.SupportType.INTEGER));
        return fieldInfos;
    }
}

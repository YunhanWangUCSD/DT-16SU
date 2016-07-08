package com.example.MyFileToJdbcApp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;


@ApplicationAnnotation(name = "MyFileToJdbcApp")
public class FileToJdbcApp implements StreamingApplication{

    @Override
    public void populateDAG(DAG dag, Configuration configuration) {

        FileReader fileReader = dag.addOperator("FileReader", FileReader.class);
        LineParser lineParser = dag.addOperator("LineParser", LineParser.class);
        JdbcPOJOInsertOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOInsertOutputOperator.class);

//        ConsoleOutputOperator consoleOutputOperator = new ConsoleOutputOperator();
//        dag.addOperator("ConsoleOutput", consoleOutputOperator);

        // configure operators
        fileReader.setDirectory("/user/yunhan");

        jdbcOutputOperator.setTablename("test_jdbc_table");
        jdbcOutputOperator.setFieldInfos(addFieldInfos());
        JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
        jdbcOutputOperator.setStore(outputStore);
        dag.setInputPortAttribute(jdbcOutputOperator.input, Context.PortContext.TUPLE_CLASS, PojoEvent.class);

        // add stream
//        dag.addStream("POJO's",csvParser.parsedOutput, jdbcOutputOperator.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
        dag.addStream("data", fileReader.output, lineParser.input);
        dag.addStream("POJO", lineParser.output, jdbcOutputOperator.input);
//        dag.addStream("test", lineParser.output, consoleOutputOperator.input);

    }

    /**
     * This method can be modified to have field mappings based on used defined
     * class
     */
    private List<JdbcFieldInfo> addFieldInfos()
    {
        List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
        fieldInfos.add(new JdbcFieldInfo("ACCOUNT_NO", "accountNumber", JdbcFieldInfo.SupportType.INTEGER , INTEGER));
        fieldInfos.add(new JdbcFieldInfo("NAME", "name", JdbcFieldInfo.SupportType.STRING, VARCHAR));
        fieldInfos.add(new JdbcFieldInfo("AMOUNT", "amount", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
        return fieldInfos;
    }
}

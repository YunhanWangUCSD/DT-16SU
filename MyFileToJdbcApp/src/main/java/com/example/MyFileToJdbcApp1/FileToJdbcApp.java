package com.example.MyFileToJdbcApp1;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.db.jdbc.JdbcPOJOOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.util.FieldInfo;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Created by YunhanWang on 6/27/16.
 */

@ApplicationAnnotation(name = "MyFileToJdbcApp1")
public class FileToJdbcApp implements StreamingApplication{

    @Override
    public void populateDAG(DAG dag, Configuration configuration) {


        String pojoSchema = SchemaUtils.jarResourceFileToString("schema.json");


        FileReader reader = dag.addOperator("FileReader", FileReader.class);
        CsvParser parser = dag.addOperator("FileParser", CsvParser.class);
        JdbcPOJOOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOOutputOperator.class);
//        ConsoleOutputOperator consoleOutputOperator = new ConsoleOutputOperator();
//        dag.addOperator("ConsoleOutput", consoleOutputOperator);



        // configure operators
        reader.setDirectory("/user/yunhan");

        parser.setClazz(PojoEvent.class);
        parser.setSchema(pojoSchema);
        dag.setOutputPortAttribute(parser.out, Context.PortContext.TUPLE_CLASS, PojoEvent.class);



        jdbcOutputOperator.setTablename("test_jdbc_table");
        jdbcOutputOperator.setFieldInfos(addFieldInfos());
        JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
        jdbcOutputOperator.setStore(outputStore);
        dag.setInputPortAttribute(jdbcOutputOperator.input, Context.PortContext.TUPLE_CLASS, PojoEvent.class);



        // add stream
        dag.addStream("byte's", reader.output, parser.in);
//        dag.addStream("test", parser.out, consoleOutputOperator.input);
        dag.addStream("POJO's", parser.out, jdbcOutputOperator.input);
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

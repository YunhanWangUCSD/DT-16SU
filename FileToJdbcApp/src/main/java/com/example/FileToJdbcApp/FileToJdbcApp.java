package com.example.FileToJdbcApp;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;


@ApplicationAnnotation(name = "FileToJdbcApp")
public class FileToJdbcApp implements StreamingApplication{

    @Override
    public void populateDAG(DAG dag, Configuration configuration) {


        // create operators
        String pojoSchema = SchemaUtils.jarResourceFileToString("schema.json");

        FileReader reader = dag.addOperator("FileReader", FileReader.class);
        CsvParser parser = dag.addOperator("FileParser", CsvParser.class);
        JdbcPOJOInsertOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOInsertOutputOperator.class);


        // configure operators
        reader.setDirectory("/user/yunhan");

        parser.setSchema(pojoSchema);
        dag.setOutputPortAttribute(parser.out, Context.PortContext.TUPLE_CLASS, PojoEvent.class);

        jdbcOutputOperator.setTablename("test_jdbc_table");
        jdbcOutputOperator.setFieldInfos(addFieldInfos());
        JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
        jdbcOutputOperator.setStore(outputStore);
        dag.setInputPortAttribute(jdbcOutputOperator.input, Context.PortContext.TUPLE_CLASS, PojoEvent.class);


        // add stream
        dag.addStream("Byte's", reader.output, parser.in);
        dag.addStream("POJO's", parser.out, jdbcOutputOperator.input);
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

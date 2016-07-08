### Application -- file to database
The `fileIO-simple` application copies all data verbatim from files added to an input
directory to rolling files in an output directory. The input and output directories,
the output file base name and maximum size are configured in `META_INF/properties.xml`:

```xml
<property>
  <name>dt.application.SimpleFileIO.operator.input.prop.directory</name>
  <value>/tmp/SimpleFileIO/input-dir</value>
</property>
<property>
  <name>dt.application.SimpleFileIO.operator.output.prop.filePath</name>
  <value>/tmp/SimpleFileIO/output-dir</value>
</property>
<property>
  <name>dt.application.SimpleFileIO.operator.output.prop.fileName</name>
  <value>myfile</value>
</property>
<property>
  <name>dt.application.SimpleFileIO.operator.output.prop.maxLength</name>
  <value>1000000</value>
</property>
```

The application DAG is created in `Application.java`:

```java
@Override
public void populateDAG(DAG dag, Configuration conf)
{
  // create operators
  FileLineInputOperator in = dag.addOperator("input", new FileLineInputOperator());
  FileOutputOperator out = dag.addOperator("output", new FileOutputOperator());

  // create streams
  dag.addStream("data", in.output, out.input);
}
```

The `FileLineInputOperator` is part of Malhar and is a concrete class that extends
`AbstractFileInputOperator`. The `FileOutputOperator` is defined locally
and extends the `AbstractFileOutputOperator` and overrides 3 methods:
+ `getFileName` which simply returns the current file name
+ `getBytesForTuple` which appends a newline to the argument string, converts it to an array of bytes and returns it.
+ `setup` which creates the actual file name by appending the operator id to the configured base name (this last step is necessary when partitioning is involved to ensure that multiple partitions do not write to the same file).

Output files are created with temporary names like `myfile_p2.0.1465929407447.tmp` and
renamed to `myfile_p2.0` when they reach the maximum configured size.



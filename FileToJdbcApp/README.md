## Sample File to JDBC application

This application reads from file(s) from the user specified input directory in HDFS, parses into POJOs and then writes them to a table in `MySQL`.

Follow these steps to run this application:

**Step 1**: 

- Update schema for CsvParser in the file `src/main/resources/schema.json`
- Update PojoEvent class and addFieldInfos() method in `src/main/java/com/example/MyFileToJdbcApp`


**Step 2**: 

- Update these properties in the file `src/site/conf/example.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.application.MyFileToJdbcApp.operator.reader.prop.directory |HDFS input directory path 
|dt.application.MyFileToJdbcApp.operator.JdbcOutput.prop.store.databaseUrl | database URL of the form `jdbc:mysql://hostName:portNumber/dbName` |
| dt.application.MyFileToJdbcApp.operator.JdbcOutput.prop.store.userName | MySQL user name |
| dt.application.MyFileToJdbcApp.operator.JdbcOutput.prop.store.password | MySQL user password |
| dt.application.MyFileToJdbcApp.operator.JdbcOutput.prop.tablename   | MySQL output table name |

**Step 3**: 

- Create input file(s). For example: 

		1, 'User1', 1000
		2, 'User2', 2000
		3, 'User3', 3000
		4, 'User4', 4000
		5, 'User5', 5000
		6, 'User6', 6000
		7, 'User7', 7000
		8, 'User8', 8000
		9, 'User9', 9000
		10, 'User10',10000
		
- Create HDFS input directory if not already present (_{path/to/input/directory}_ should be the same as specified in `META_INC/properties.xml`):

    	hdfs dfs -mkdir {path/to/input/directory}

- Put input file(s) into HDFS input directory
		
		hdfs dfs -put {path/to/file} {path/to/input/directory}

**Step 4**:

- Create database table. Go to the MySQL console and run, for example:

    	mysql> create table test_jdbc_table(ACCOUNT_NO INTEGER(11) NOT NULL,
    		-> NAME VARCHAR(255),
    		-> AMOUNT INTEGER(11));

- After this, please verify that `testDev.test_jdbc_table` is created and has 3 rows:

    	mysql> desc test_jdbc_table;
		+------------+--------------+------+-----+---------+-------+
		| Field      | Type         | Null | Key | Default | Extra |
		+------------+--------------+------+-----+---------+-------+
		| ACCOUNT_NO | int(11)      | NO   |     | NULL    |       |
		| NAME       | varchar(255) | YES  |     | NULL    |       |
		| AMOUNT     | int(11)      | YES  |     | NULL    |       |
		+------------+--------------+------+-----+---------+-------+
		3 rows in set (0.00 sec)


**Step 4**: 

- Build the code:

    	shell> mvn clean install

- Upload the `target/MyFileToJdbcApp-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apexcli`.

**Step 5**: 

- During launch use `site/conf/example.xml` as a custom configuration file; 
- After running successfully, verify
that the database table has the expected output: 
	
		mysql> select * from test_jdbc_table;
		+------------+--------+--------+
		| ACCOUNT_NO | NAME   | AMOUNT |
		+------------+--------+--------+
		|          1 | User1  |   1000 |
		|          2 | User2  |   2000 |
		|          3 | User3  |   3000 |
		|          4 | User4  |   4000 |
		|          5 | User5  |   5000 |
		|          6 | User6  |   6000 |
		|          7 | User7  |   7000 |
		|          8 | User8  |   8000 |
		|          9 | User9  |   9000 |
		|         10 | User10 |  10000 |
		+------------+--------+--------+
		10 rows in set (0.00 sec)

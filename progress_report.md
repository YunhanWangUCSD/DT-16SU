##Progress Report    
Think More. Do More. Learn More.


#### Week 07/18-07/22
Goals:

1. finish log analyzer subtask: 

		(1) understand current scripts, get RM log and AM log using REST API
		(2) parse the AM log: extract any exceptions, figure out where the containers were created and use the REST API to get logs from those nodes

Finished:

1. Fixed File to Jdbc PR comments, okay to merge now
2. 

TODO:

1. Start log analyzer task


#### Week 07/18-07/22
Goals:

1. Resolve file to jdbc example task, PR merge
2. log analyzer subtasks:

		(1) understand current scripts, get RM log and AM log using REST API
		(2) parse the AM log: extract any exceptions, figure out where the containers were created and use the REST API to get logs from those nodes

Finished:

1. Opened File to Jdbc PR, fixed according to comments

TODO:

1. Start log analyzer task


#### Week 07/11-07/15
Goals:

1. modify code based on review commends from Ram
2. solve mysql primary key problem from Pradeep
3. write my own jdbc update and insert operator extends jdbc insert operator

Finished:

1. Created APEXMALHAR-2140 JIRA: Move ActiveFieldInfo class to com.datatorrent.lib.util
2. Finished partial jdbc update and insert operator code and test code, sent to Dev

TODO:

1. Open PR for file to jdbc example 


#### Week 07/05-07/08
Goals:

1. write README.md for my application
2. write local test for my application
3. pull my application code to `tutorial/example` github repo
4. code review and wirte beginner's guide part
5. start learning python code to find down workers

Finished:

1. write README.md for my application
2. write local test for my application
3. merge two application into one, can choose which to launch in console

TODO:

1. modify code based on review commends from Ram
2. need to solve mysql primary key question from Pradeep

#### Week 06/27-07/01
Finished:

1. finish building my own file to jdbc application, run successfully on console
2. answer customer's question on JdbcPojoOutputOperator on mail list


#### Week 06/20-06/24
Weekly Goals:

1. finish follow at least two apache apex application demos
2. start my own application building


####06/20 Mon
Finished:

1. finished Apache Apex environment setting
2. followed "build your first apache apex application" tutorial
3. finished until IntelliJ my own new project unit test pass

TODO:

1. fix IntelliJ error 
2. fix changes in mahar/demos/..
3. continue learn topnwordcount app

####06/21 Tue
Finished:

1. fixed IntelliJ error 
2. fixed changes in mahar/demos/..
3. finished topnwordcount demo building in IntelliJ
4. finished building top N words using dtAssemble
5. finished monitoring top N words using dtManage


TODO:

1. finish Visualizing the application output using dtDashboard
2. read <http://docs.datatorrent.com/application_development/>
3. read beginner.md

####06/22 Wed
Finished:

1. learn how to read log files
2. add/adjust ports of operators in java project


TODO:
1. solve mynewapp project errors
		
		dag.addStream("WordCountsFile", fileWordCount.outputPerFile,
              snapshotServerFile.input, console.input);
      	dag.addStream("WordCountsGlobal", fileWordCount.outputGlobal,
                    snapshotServerGlobal.input);
                    

####06/23 Thu
Finished:

1.  solve mynewapp project errors
2.  finish reading beginner.md
3.  finish trying serveral examples: unifier, partition, wordcount
		

TODO:

1. check video: build project on sandbox
2. sandbox edition problem
3. share my .md through new github repo with Ram
4. Clone the Apex source code on a cluster with Hadoop already installed, build it and use the apexcli command line tool 
5. visulization tool check


####06/24 Fri
Finished:

1.  listen to presentations

TODO:

1. check video: build project on sandbox
2. sandbox edition problem
3. share my .md through new github repo with Ram
4. Clone the Apex source code on a cluster with Hadoop already installed, build it and use the apexcli command line tool 
5. visulization tool check


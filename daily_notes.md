##Daily Notes    
Think More. Do More. Learn More.

####07/21 Thu

- simple git cheatsheet: <https://gist.github.com/hofmannsven/6814451>


####07/07 Thu

- useful commands to update cloned github repo:

		git status
		git log
		git remote -v
		git pull
		mvn-pkg
		mvn clean install -DskipTests
		git branch
		git stash
		git diff
		

####06/29 Wed

- mysql commands: <https://www.digitalocean.com/community/tutorials/a-basic-mysql-tutorial>
			
		mysql> truncate tablename; to empty a table!
		

- vim commands: <http://vim.wikia.com/wiki/Search_and_replace>

		Type dG to delete all the lines.
		Type gg to move the cursor to the first line of the file, if it is not already there.
		Type :noh to turn off highlighting until the next search.
		Type set nohlsearch to turn off highlighting completely.
		Type gg"*yG to yank the text.
		Type :%y+ to yank all lines.



####06/27 Mon

- ssh to lab:

		ssh-keygen -t rsa -C
		ssh-keygen -t rsa
		cd .ssh
		vi id_rsa.pub
	<https://confluence.atlassian.com/bitbucketserver/creating-ssh-keys-776639788.html#CreatingSSHkeys-CreatinganSSHkeyonLinux&MacOSX>
	<https://confluence.atlassian.com/bitbucketserver/creating-ssh-keys-776639788.html#CreatingSSHkeys-CreatinganSSHkeyonLinux&MacOSX>

####06/24 Fri

- JdbcIngestion app:
	
		
		1. some useful commands to install and run mysql in sandbox:
		dpkg -l | grep -i mysql
  		apt-cache show mysql
  		apt-cache show mysql*
  		apt-cache show mysql* | grep Package
  		sudo apt-get install mysql-server
		apt-get update
		sudo apt-get install mysql-client
		mysql -u root -p

		2. dpkg -l | grep -i mysql
  		   apt-cache show mysql
		   apt-cache show mysql*
		   apt-cache show mysql* | grep Package
  		   sudo apt-get install mysql
  	           sudo apt-get install mysql-server
  		   sudo apt-get install mysql-server
		   sudo apt-get update
		   cd /etc/apt
		   sudo apt-get update
		   sudo apt-get install mysql-server
		   sudo apt-get install mysql-client
		   history |grep aptpkg
		   history |grep apt

		
####06/22 Wed

- when logging into Sandbox console, red HDFS persists for a long time:
	
		
		1. check the status of each of these following services:
		#!/bin/bash
		services='hadoop-hdfs-namenode hadoop-hdfs-datanode hadoop-		yarn-resourcemanager hadoop-yarn-nodemanager dtgateway'
		for s in $services; do
    		sudo service "$s" status
		done

		2. start all services again: bash services start
		3. stop all services: bash services stop
		4. reformat: hdfs namenode -format
		5. restart and check if anything missing:
			~$ jps
			8041 NodeManager
			7668 DataNode
			7758 ResourceManager
			8219 DTGateway
			8390 Jps
			7564 NameNode
		6. try some write operation: 
			hdfs dfs -mkdir /tmp
		



####06/21 Tue

- a smarter way to create a newapp maven project:
	
		
		$ bash newapp
		you can change new project names inside this script
		under testapp/src/main/java/com/example/testapp/
		it will give you two original java files: 
		Application.java 		
		RandomNumberGenerator.java
		under testapp/src/test/java/com/example/testapp/
		it will give you one original java files: 
		ApplicationTest.java

- if you need to run your own app on sandbox, you only need to copy the `.apa` file created by your project to the `share` folder.

- some smart shell script:

		change dir smartly: pushd ~ and popd ~
		search in file: grep -i datatorrent pom.xml 
		add alias: alias mvn-pkg="mvn clean package -DskipTests"
		or change in bash_profile:vi .bash_profile
		
- some hdfs dfs shell commands:
<https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-common/FileSystemShell.html>
 
		find all items in hdfs: hdfs hdfs dfs -ls /
		different from: ls /
		make new dir: hdfs dfs -mkdir /foo
		put file from local to hdfs: hdfs dfs -put test.txt /foo
		get file from hdfs to local: hdfs dfs -get /foo/test.txt /
		remove file: hdfs dfs -rm /foo/test.txt
		remove dir: hdfs dfs -rmdir /foo

- IntelliJ new project: pom.xml 
		
  `Solution:`
		
		change from
		Group ID	com.datatorrent
		to 
		Group ID	org.apache.apex

- shared folder stopped sync 

	`Solution:`
	<https://help.ubuntu.com/community/VirtualBox/SharedFolders>
	
		in virtual machine terminal do this:
		~$ sudo mount -t vboxsf -o uid=$UID,gid=$(id -g) share ~/share
		


- Launch TopNWordsWithQueries 


		An error occurred trying to launch the application. Server 		message: com.datatorrent.stram.cli.DTCli$CliException: This 		App Package is compiled with Apache Apex Core API version 		3.5.0-SNAPSHOT, which is incompatible with this Apex Core 		version 3.3.1-dt20160316 at com.datatorrent.stram.cli.DTCli.checkPlatformCompatible(DTCli.java:3450) at com.datatorrent.stram.cli.DTCli.access$6900(DTCli.java:106) at com.datatorrent.stram.cli.DTCli$LaunchCommand.execute(DTCli.java:1892) at com.datatorrent.stram.cli.DTCli$3.run(DTCli.java:1449)
		
	`Solution:
	recreate .apa with newapp project`
	
	

####06/20 Mon 

- environment set up:

		git clone https://github.com/apache/incubator-apex-core
		git clone https://github.com/apache/incubator-apex-malhar
		cd incubator-apex-core
		mvn clean install -DskipTests
		cd ../incubator-apex-malhar/
		mvn clean install -DskipTests
		
- Building you own Apex application:
	
		in populateDAG, we need to add few lines to link input port 		
		and output port.

- Webinar: Building Your First Apache Apex Application 


		video ~30 min
		dtadmin@dtbox:~/share/apex$ incubator-apex-core/engine/src/main/	scripts/dtcli
		bash: incubator-apex-core/engine/src/main/scripts/dtcli: No such 	file or directory
	
	`Solution: change dtcli to apex`





		

To run the actual project, use the TestFolder.  This contains the compiled code.
Go into the UI folder and place all of this onto the machine you are running the cluster from.
The data files such as Shakespear should be placed into a directory which is calld "ProjectTestData".

Once all this is done, do the following:
1. run "java RunHadoopServer" from the command line of the cluster machine, from wherever you 
copied the files to
2. on your local machine, you will have to pull the docker image by using:
	docker pull mas682/cloudsproject:project
	(the link can be found here: https://hub.docker.com/r/mas682/cloudsproject/tags)
3. then run docker run -i mas682/cloudsproject:project

notes:
CloudComputingProject holds the code for the inverted indexing implementation
CloudComputingSearch holds the code for the search term implementation
CloudComputingSrc holds the code for the user interfaces, connecting the applications
CloudComputingTopN holds the code for the top N implementation
hadoop.sh is the shell script to run inverted indexing
hadoopSearch.sh is the shell script to run the search for term on hadoop
hadoopTopN.sh is the shell script to run the topN program
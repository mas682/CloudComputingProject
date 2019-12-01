hadoop fs -rm -r outputSearch
rm -r SearchData/searchResults
mkdir SearchData
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
jar cvf SearchTerm.jar -C SearchTerm/ .
hadoop jar SearchTerm.jar SearchTerm -D mapreduce.task.profile=true InvertedIndexData outputSearch -T $1
hadoop fs -getmerge outputSearch SearchData/searchResults

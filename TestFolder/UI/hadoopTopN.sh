hadoop fs -rm -r outputTopN
rm -r TopNData/TopNResults
mkdir TopNData
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
jar cvf TopN.jar -C TopN/ .
hadoop jar TopN.jar TopN -D mapreduce.task.profile=true InvertedIndexData outputTopN -N $1
hadoop fs -getmerge outputTopN TopNData/TopNResults

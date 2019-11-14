hadoop fs -rm -r outputSearch
rm -r SearchData/searchResults
mkdir SearchDataexport JAVA_HOME=/usr/local/jdk1.8.0_101
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
jar cvf SearchTerm.jar -C SearchTerm/ .
hadoop jar SearchTerm.jar SearchTerm InvertedIndexData outputSearch -T $1
hadoop fs -getmerge outputSearch SearchData/searchResults

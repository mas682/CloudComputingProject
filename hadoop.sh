rm -r InvertedIndexData/collectedResults5
mkdir InvertedIndexData
hadoop fs -rm -r output71
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
jar cvf InvertedIndexing.jar -C InvertedIndexing/ .
hadoop fs -copyFromLocal ProjectTestData/ .
hadoop fs -cp ProjectTestData/ gs://dataproc-10a0c283-9abf-42e4-b48a-c3b6a0c4df52-us-west1
hadoop jar InvertedIndexing.jar InvertedIndexing ProjectTestData output71
hadoop fs -getmerge output71 InvertedIndexData/collectedResults5
hadoop fs -copyFromLocal InvertedIndexData/ .
hadoop fs -cp InvertedIndexData/ gs://dataproc-10a0c283-9abf-42e4-b48a-c3b6a0c4df52-us-west1





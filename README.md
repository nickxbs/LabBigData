# LabBigData

Finding triangles algorithm using Hadoop framework

## Prerequisites

Install JDK 1.8 and Hadoop framework

## Run

```
export LIBJARS=LabBigData/lib/javatuples-1.2.jar
export HADOOP_CLASSPATH=LabBigData/lib/javatuples-1.2.jar

hadoop jar prjTriangleAdv.jar -libjars ${LIBJARS} -i "hdfs_inputfile.txt" -o "hdfs_outoutfolder" -b numberofbucket

```


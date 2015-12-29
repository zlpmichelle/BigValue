echo "Start at `date`"
CLASSPATH=.:./generalETLTool.jar
jars=`ls ./lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:./lib/$jar"
done

CLASSPATH=/usr/lib/hadoop/conf:/usr/lib/hbase/conf:/usr/lib/hive/conf:$CLASSPATH

# Here we create a table named 'sourcetable' in hbase and put some test data. Notice that the value of col3 and col5 is empty in this table.

echo "get 'sourcetable','row1'" | hbase shell
hadoop fs -text /user/dachao/etltest/hdfssource/hdfssourcedata.txt

java -Djava.library.path=/usr/lib/hadoop/lib/native/Linux-amd64-64 -cp $CLASSPATH org.junit.runner.JUnitCore com.cloudera.etl.GeneralETLToolTest

# Check after update
echo "get 'targettable','row1'" | hbase shell
echo "scan 'hdfs2hbasetargettable'" | hbase shell

echo "End at `date`"

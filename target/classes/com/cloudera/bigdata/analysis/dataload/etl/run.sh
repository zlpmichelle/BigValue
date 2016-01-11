echo "Start at `date`"
CLASSPATH=.:./generalETLTool.jar
jars=`ls ./lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:./lib/$jar"
done

CLASSPATH=/usr/lib/hadoop/conf:/usr/lib/hbase/conf:/usr/lib/hive/conf:$CLASSPATH

# Here we create a table named 'sourcetable' in hbase and put some test data. Notice that the value of col3 and col5 is empty in this table.
echo "disable 'sourcetable'" | hbase shell
echo "drop 'sourcetable'" | hbase shell
echo "create 'sourcetable','cf'" | hbase shell
echo "put 'sourcetable','row1','cf:col1','val1'" | hbase shell
echo "put 'sourcetable','row1','cf:col2','val2'" | hbase shell
echo "put 'sourcetable','row1','cf:col3',''" | hbase shell
echo "put 'sourcetable','row1','cf:col4','val4'" | hbase shell
echo "put 'sourcetable','row1','cf:col5',''" | hbase shell
echo "put 'sourcetable','row1','cf:col6','val6'" | hbase shell

echo "put 'sourcetable','row1','cf:col7','val7 whitespace	'" | hbase shell
echo "put 'sourcetable','row1','cf:col8','val8 123assume345contribute'" | hbase shell
echo "put 'sourcetable','row1','cf:col9','val9 123assume345contribute \t !@#$% \n sfssfslll'" | hbase shell
echo "put 'sourcetable','row1','cf:col10','lidach \t dafa \n jjj 13524306691lidad'" | hbase shell
echo "put 'sourcetable','row1','cf:col11','lidach \t dafa \n jjj 135243066918lidad'" | hbase shell
echo "put 'sourcetable','row1','cf:col12','da_123 456 \r\nsfsf'" | hbase shell
echo "put 'sourcetable','row1','cf:col13','���" | hbase shell

echo "get 'sourcetable','row1'" | hbase shell
hadoop fs -rmr /user/dachao/etltest
hadoop fs -mkdir /user/dachao/etltest
hadoop fs -mkdir /user/dachao/etltest/hdfssource
hadoop fs -copyFromLocal /root/dachao/GeneralETLTool/data/hdfssourcedata.txt /user/dachao/etltest/hdfssource
hadoop fs -text /user/dachao/etltest/hdfssource/hdfssourcedata.txt

java -Djava.library.path=/usr/lib/hadoop/lib/native/Linux-amd64-64 -cp $CLASSPATH org.junit.runner.JUnitCore com.cloudera.etl.GeneralETLToolTest

# Check after update
echo "get 'targettable','row1'" | hbase shell
echo "scan 'hdfs2hbasetargettable'" | hbase shell

echo "End at `date`"

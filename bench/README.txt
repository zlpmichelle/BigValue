1. Build Project
    sh buildycsb.sh

    it will generate the ycsb.tmp folder

2. Run bench
    1) make sure the hadoop related packages (hadoop-core-*.jar, hbase-*.jar, zookeeper-*.jar) in ycsb.tmp/hbase-binding/lib are consistent with those in your hadoop cluster
    
    2) change to ycsb.tmp directory, and do the test

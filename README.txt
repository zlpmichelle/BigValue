BigValue is a distributed big data analysis system over Cloudera Distributed Hadoop(CDH).

------------------------
The delivery for customer:
1.Run $BIGVALUE_HOME/build.sh, it will generated a obfuscator codes package under $BIGVALUE_HOME/target/BigValue-1.0.tar.gz
2.Bring the $BIGVALUE_HOME/target/BigValue-1.0.tar.gz to cluster side.



The latest BigValue can be downloaded from an BigValue Mirror [1].

The source code can be found at [1]

The BigValue changes tracker is at [2]

1. BigValue
2. BigValue/CHANGES.txt

BUILD
====================
Apache-ANT:
  You should have jdk and apache-ant installed
  ant -Dcdh.version=<version> package
      version can be 2.5.1 or 3.1

Maven
  You should have jdk and maven installed in build machine.
  
  Build Steps: 
  Copy maven/setting.xml to ${USER.HOME}/.m2/
  Type mvn package to build project



RUN
====================
To get started using BigValue's different components, like BulkLoad, DataGen, etc., the full documentation for this release can be found under the docs/ directory that accompanies this README.

Also, you can find some pre-built examples in examples folder to understand the usage directly.
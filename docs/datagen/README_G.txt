===============================
**********DESCRIPTION**********
===============================

Generator is supposed to help you generate some sample data in MultiThreading mode or MapReduce mode.

The generation speed is rather related to disk parallelism, especially in mapred mode, the homo system (each with same disk number and disk IO bandwidth) will get the best performance.

The main entry is datagen/src/java/main/com/cloudera/bigdata/analysis/datagen/GeneratorDriver.java. 

To use this generator, you must prepare an xml configuration file. It must follows the schema definition in schema/generator.xsd. There is an example file called generator.xml in examples folder.

In one replication mode, normally the generation speed for raw data will be faster than compression
While in multiple replication mode, compression will be better.

Then, you can invoke com.cloudera.bigdata.analysis.datagen.GeneratorDriver, it has the following options, (*) is required:

  --instanceDoc(*) the xml configuration file

  --outputDir(*)   the absolution output directory for generator:
                   for windows local, it should be specified as 'file:///f:/xxx';
                   for linux local, it should be specified as 'file:///tmp/xxx';
                   for HDFS, should be specified as hdfs://<namenode>:8020/xxx, make sure the folder is writable.
  
  --replicaNum     number of replica in HDFS, should be a number between 1 and 3, the default value is 1
  
  --codec          configure compression codec for generated source, gzip, snappy, lz4 are supported compressions.
   
  --mode           the running mode for generator, 'mapred' is default; any other argument then mapred will be multithreading mode

  --parallel       the parallelism for generator, in mapred mode, is the expected mapper number, in multithreading mode, is the thread num, defalut is 1


  --totalSize      the total size of dataset to be generated, default is 1(GB)

  --minSize        the minumum size of a single generated file, default is 10(MB)

  --maxSize        the maximum size of a single generated file, default is 100(MB), it must be no smaller than minsize

  
  --totalNum       total number of records need to be generated, default is 100000000

  --minNum         minimum number of records generated for a single file, defaule is 1000000
 
  --maxNum         maximum number of records generated for a single file, default is 10000000

  --debug          switch for debug mode, default is false

  --neverStop      switch for streaming dataload, default is false

More info for xml configuration file, now it supports following data types:
  
  type             sample

  DATETIME         <column>
			         <name>time_id</name>
			         <start>201212230000</start>
			         <end>201212231000</end>
			         <pattern>YYYYMMDDHHmm</pattern>
			         <randomType>DATETIME</randomType>
	               </column>

  
  LONG             <column>
			         <name>filesize_rnk_cd</name>
			         <start>0</start>
			         <end>4</end>
			         <randomType>LONG</randomType>
	      	       </column>

  PHONE            <column>
			         <name>phone_num</name>
			         <start>13300000000</start>
			         <end>18900000000</end>
			         <randomType>PHONE</randomType>
	               </column>

 *CORRELATE        <column>
			         <name>city_id</name>
			         <ref>
				       <refColumn>phone_num</refColumn>
				       <refOp>SUBSTRING</refOp>
				       <refStart>3</refStart>
				       <refEnd>6</refEnd>
			         </ref>
			         <randomType>CORRELATE</randomType>
	  	           </column>

  CONSTANT         <column>
			         <name>code_id</name>
			         <value>61</value>
			         <randomType>CONSTANT</randomType>
               	   </column>

  STRING           <column>
			         <name>rand_id</name>
			         <length>32</length>
			         <randomPattern>ALPHABET</randomPattern>
			         <randomType>STRING</randomType>
	               </column>

  URL              <column>
			         <name>url</name>
			         <value>172-234.16-168.1-10.1-255</value>
			         <randomType>URL</randomType>
	               </column>

 *UDF              <column>
			         <name>service_type</name>
			         <udfName>com.cloudera.bigdata.analysis.custom.datagen.ServiceTypeGenerator</udfName>
			         <randomType>UDF</randomType>
	               </column>

=======================
**********RUN**********
=======================
You can refer to the run command in examples folder.
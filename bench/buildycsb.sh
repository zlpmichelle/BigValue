#!/bin/sh

YCSBProject=YCSBCore
YCSBPackage=core-0.1.4.jar
HBaseProject=HBaseBinding
HBasePackage=hbase-binding-0.1.4.jar
TempDir=ycsb.tmp

if [ -e $TempDir ];
then
  rm -rf $TempDir
fi

mkdir -p $TempDir/core/lib
mkdir -p $TempDir/hbase-binding

# Build ycsb core
CURDIR=`pwd`
cd $YCSBProject
ant package
cd $CURDIR
# Copy ycsb related to temp folder
cp -r $YCSBProject/ycsb-bin $TempDir/bin
cp -r $YCSBProject/workloads $YCSBProject/CHANGELOG $TempDir
cp $YCSBProject/target/$YCSBPackage $TempDir/core/lib

# Build hbase binding
cp $YCSBProject/target/$YCSBPackage $HBaseProject/lib/
cd $HBaseProject
ant package
rm lib/$YCSBPackage
cd $CURDIR

# Copy hbase related to temp folder
cp -r $HBaseProject/conf $HBaseProject/lib $TempDir/hbase-binding
cp $HBaseProject/target/$HBasePackage $TempDir/hbase-binding/lib

# Package ycsb
cd $TempDir
tar czf ycsb-0.1.4.tar.gz *




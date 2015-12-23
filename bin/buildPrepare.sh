#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)

# include running cluster conf/, lib/ and also native/
confandlibpath=$currentpath/../../confandlib


cp -rfv $confandlibpath/conf/* $currentpath/../conf/
cp -rfv $confandlibpath/lib/* $currentpath/../lib/lib_cdh5.0
chmod +x $currentpath/../*.sh $currentpath/*.sh   
$currentpath/../build.sh $@                                                                        
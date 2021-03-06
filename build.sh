#!/bin/sh
if [ $# != 1 ];then
    echo "Usage: buils.sh <cdh_version>"
fi

. ./bin/function.sh

#if ./bin/checklib.sh ; then
#    echo "Check lib [PASS]"
#else
#    continue_ask
#fi

if [ $1 = "5.0" ];then
    cp 5.0.classpath .classpath
else
    cp 5.1.classpath .classpath
fi

ant clean
ant package

currentpath=$(cd "$(dirname "$0")";pwd)
cd $currentpath/target/ 
tar zcf BigValue-1.0.tar.gz BigValue-1.0 --exclude BigValue-1.0/build.sh

currentpath=`pwd`
echo "The Released BigValue is generated in $currentpath/BigValue-1.0.tar.gz"

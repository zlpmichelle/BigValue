#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)

bigdataanalysisname=ASB
deplyASBhome=/root/zhangliping

# local package
cd $currentpath/../../
echo `pwd`
echo $bigdataanalysisname
tar zcf BigDataAnalysis.tar.gz $bigdataanalysisname

# copy to other node
for i in "${nodelist[@]}"
do
    echo clean $deplyASBhome/$bigdataanalysisname on $i
    ssh $i "rm -rf \"$deplyASBhome\"\/\"$bigdataanalysisname\""
  
	echo mkdir to $i:$deplyASBhome
	ssh $i "mkdir \"$deployASBhome\""
	
	echo copy BigDataAnalysis.tar.gz to $i:$deplyASBhome
	scp BigDataAnalysis.tar.gz root@$i:$deplyASBhome
	
    echo uppack BigDataAnalysis.tar.gz to $i:$deplyASBhome
	ssh $i "cd \"$deplyASBhome\" && tar zxf BigDataAnalysis.tar.gz"	
done


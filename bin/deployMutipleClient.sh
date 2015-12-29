#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)

bigvaluename=ASB
deplyASBhome=/root/zhangliping

# local package
cd $currentpath/../../
echo `pwd`
echo $bigvaluename
tar zcf BigValue.tar.gz $bigvaluename

# copy to other node
for i in "${nodelist[@]}"
do
    echo clean $deplyASBhome/$bigvaluename on $i
    ssh $i "rm -rf \"$deplyASBhome\"\/\"$bigvaluename\""
  
	echo mkdir to $i:$deplyASBhome
	ssh $i "mkdir \"$deployASBhome\""
	
	echo copy BigValue.tar.gz to $i:$deplyASBhome
	scp BigValue.tar.gz root@$i:$deplyASBhome
	
    echo uppack BigValue.tar.gz to $i:$deplyASBhome
	ssh $i "cd \"$deplyASBhome\" && tar zxf BigValue.tar.gz"
done


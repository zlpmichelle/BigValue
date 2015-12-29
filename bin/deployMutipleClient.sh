#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)
nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)

bigvaluename=EXAMPLE1
deplyEXAMPLE1home=/root/zhangliping

# local package
cd $currentpath/../../
echo `pwd`
echo $bigvaluename
tar zcf BigValue.tar.gz $bigvaluename

# copy to other node
for i in "${nodelist[@]}"
do
    echo clean $deplyEXAMPLE1home/$bigvaluename on $i
    ssh $i "rm -rf \"$deplyEXAMPLE1home\"\/\"$bigvaluename\""
  
	echo mkdir to $i:$deplyEXAMPLE1home
	ssh $i "mkdir \"$deployEXAMPLE1home\""
	
	echo copy BigValue.tar.gz to $i:$deplyEXAMPLE1home
	scp BigValue.tar.gz root@$i:$deplyEXAMPLE1home
	
    echo uppack BigValue.tar.gz to $i:$deplyEXAMPLE1home
	ssh $i "cd \"$deplyEXAMPLE1home\" && tar zxf BigValue.tar.gz"
done


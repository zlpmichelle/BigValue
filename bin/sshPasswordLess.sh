#!/bin/sh

currentpath=$(cd "$(dirname "$0")";pwd)

nodelist=(192.168.0.121 192.168.0.122 192.168.0.123)     


# generate local ssh rsa key
ssh-keygen -t rsa
localpub=$(cat /root/.ssh/id_rsa.pub)
echo $localpub

#ssh $i "echo '`sed "s/'/'\\\\\\\\''/g" <<< \"$NEW_PASSWORD\"`'"

# config ssh passwd less
for i in "${nodelist[@]}"
do
	# create authorized_keys files
	ssh $i "touch /root/.ssh/authorized_keys"

	# change chmod 
	ssh $i "chmod 644 /root/.ssh/authorized_keys"

	# cat rsa pub key into authorized_keys	
	ssh $i "echo \"$localpub\" >> /root/.ssh/authorized_keys"

	echo "ssh config passwd less on $i "
	
done     



# validation
for i in "${nodelist[@]}"
do
	# ssh to remote
	ssh $i
    echo "successful passwd less on $i "	
done    

                                                   
# IS COPIED BY am-copy.sh TO THE CLUSTER AND USED THERE

for i in $(cat otherservers); do
	ssh-keyscan $i >> ~/.ssh/known_hosts
	rsync -avz timely $i:
	rsync -avz watermarks $i:
	rsync -avz data $i:
done

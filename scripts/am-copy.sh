#!/bin/sh

# COPY EVERYTHING TO AMAZON NODES

# public ip of job manager node
IP=52.59.43.231

# private ip of job manager node
INTERNAL_IP=172.31.16.124

sed "s/JOBMANAGER_PLACEHOLDER/${INTERNAL_IP}/g" flink-conf.yaml > flink-conf-amazon.yaml

rsync -avz repos/time-killer/flink-dist/target/flink-1.2-TIMELY-bin/flink-1.2-TIMELY/ -e ssh ubuntu@${IP}:timely
rsync -avz repos/examples-timely/target/pagerank-1.0-SNAPSHOT.jar -e ssh ubuntu@${IP}:timely

rsync -avz repos/time-killer-watermarks/flink-dist/target/flink-1.2-WATERMARKS-bin/flink-1.2-WATERMARKS/ -e ssh ubuntu@${IP}:watermarks
rsync -avz repos/examples-watermarks/target/pagerank-1.0-SNAPSHOT.jar -e ssh ubuntu@${IP}:watermarks

scp flink-conf-amazon.yaml ubuntu@${IP}:timely/conf/flink-conf.yaml
scp slaves-amazon ubuntu@${IP}:timely/conf/slaves

scp flink-conf-amazon.yaml ubuntu@${IP}:watermarks/conf/flink-conf.yaml
scp slaves-amazon ubuntu@${IP}:watermarks/conf/slaves
scp slaves-amazon ubuntu@${IP}:otherservers

# DISTRIBUTE EVERYTHING TO THE TASK MANAGER SERVERS
scp copytoothers.sh ubuntu@${IP}:
ssh ubuntu@${IP} ./copytoothers.sh

#!/bin/bash

# TO BE RUN EITHER IN THE TIMELY OR THE WATERMARKS DIRECTORY ON THE SERVER

./run.sh &> /dev/null

for insts in {5,10,15,20}; do
  echo "pagerank 1 $insts"
  bin/flink run -c mariusexamples.StreamingPageRank pagerank-1.0-SNAPSHOT.jar 1 42 $insts /home/marius/results/timely/pagerank /home/marius/data/cache/data2 > /dev/null
  ./run.sh &> /dev/null
done

echo " "

for win in {1,2,3,4,5,6}; do
  echo "pagerank $win 20"
  bin/flink run -c mariusexamples.StreamingPageRank pagerank-1.0-SNAPSHOT.jar $win 42 20 /home/marius/results/timely/pagerank /home/marius/data/cache/data2 > /dev/null
  ./run.sh &> /dev/null
done

echo " "

for insts in {5,10,15,20}; do
  echo "cc 1 $insts"
  bin/flink run -c mariusexamples.StreamingConnectedComponents pagerank-1.0-SNAPSHOT.jar 1 42 $insts /home/marius/results/timely/cc /home/marius/data/cache/data2 > /dev/null
  ./run.sh &> /dev/null
done

echo " "

for win in {1,2,3,4,5,6}; do
  echo "cc $win 20"
  bin/flink run -c mariusexamples.StreamingConnectedComponents pagerank-1.0-SNAPSHOT.jar $win 42 20 /home/marius/results/timely/cc /home/marius/data/cache/data2 > /dev/null
  ./run.sh &> /dev/null
done

bin/stop-cluster.sh &> /dev/null

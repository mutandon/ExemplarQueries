#!/bin/bash

DATA_DIR='./InputData/freebase'
GRAPH_DATA=${DATA_DIR}'/freebase'

cut -f 1,3  ${GRAPH_DATA}-labels.tsv | sed -E "s/([0-9]+)\W(.+)/\2 \1/" > ${GRAPH_DATA}-labels-mid.txt

echo "Compute label frequencies"
cut -f3 -d' ' ${GRAPH_DATA}-sout.graph | sort | uniq -c | sort -nr | sed -E "s/^[ ]*([0-9]+) ([0-9]+)/\2 \1/g" > ${GRAPH_DATA}-label-frequencies.csv
echo "Done!"

echo "Compute Label Index"
mkdir -p ${DATA_DIR}/nodes-hash/
java -Xms35g -Xmx175g -jar -XX:-UseGCOverheadLimit -XX:+UseG1GC ./ExQ.jar ComputeBitsetLvl1 -kb ${GRAPH_DATA} -h ${DATA_DIR}/nohubs.txt -lf ${GRAPH_DATA}-label-frequencies.csv -dir ${DATA_DIR}/nodes-hash/


java -Xms35g -Xmx175g -jar -XX:-UseGCOverheadLimit -XX:+UseG1GC ./ExQ.jar ComputeBitsetLvl1 -kb ${GRAPH_DATA} -h ${DATA_DIR}/nohubs.txt -lf ${GRAPH_DATA}-label-frequencies.csv -dir ${DATA_DIR}/nodes-hash/ --test
echo "Done!"

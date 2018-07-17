#!/bin/bash
DATA_DIR='./InputData/freebase'
GRAPH_DATA=${DATA_DIR}'/freebase'
OUT_DIR='./OutputData'
CMD='TestExemplar'

mkdir -p $OUT_DIR

java -Xms35g -Xmx175g -jar -XX:-UseGCOverheadLimit -XX:+UseG1GC ./ExQ.jar $CMD \
 -kb ${GRAPH_DATA} \
 -d ${DATA_DIR} \
 -h ${DATA_DIR}/nohubs.txt \
 -lf ${GRAPH_DATA}-label-frequencies.csv \
 -q ${DATA_DIR}/queries/queries-journal \
 -qout ${OUT_DIR} \
 --labels ${GRAPH_DATA}-labels-mid.txt \
 -r 1 --memory 160701 --cores 20 --logLevel debug \
 -rank InputData/freebase/freebase-nodes-scores.tsv
#!/bin/bash
set -eu -o pipefail
IFS=$'\n\t'

# Use scripts/download.py
# or manually download the required files,
# their id n GDrive are listed in scripts/files.list,
# move them in $DATA_DIR

DATA_DIR='./ExQ/InputData/freebase'
SOURCE_DIR=`dirname "$(readlink -f $0)"`

python $SOURCE_DIR/download.py $SOURCE_DIR/files.list

mkdir -p ${DATA_DIR}

if [[ ! -f ${DATA_DIR}/freebase-labels.tsv ]]; then
    mv freebase-labels.tsv  ${DATA_DIR}/freebase-labels.tsv
fi

touch ${DATA_DIR}/nohubs.txt

if [[ ! -f ${DATA_DIR}/freebase-sout.graph ]]; then
    echo "Decompressing graph file"
    tar -xzvf freebase-sout.graph.tar.gz  -C  ${DATA_DIR}/
    rm -i -v freebase-sout.graph.tar.gz
    echo "Sorting edges file"
    LANG=en_EN sort -n -k2,2 ${DATA_DIR}/freebase-sout.graph > ${DATA_DIR}/freebase-sin.graph
fi

if [[ ! -f ${DATA_DIR}/freebase-nodes-in-out-name.tsv ]]; then
    echo "Decompressing node labels file"
    tar -xzvf freebase-nodes-in-out-name.tsv.tar.gz  -C  ${DATA_DIR}/
    rm -i -v freebase-nodes-in-out-name.tsv.tar.gz
fi

if [[ ! -f ${DATA_DIR}/freebase-nodes-scores.tsv ]]; then
    echo "Decompressing node scores file"
    tar -xzvf freebase-nodes-scores.tsv.tar.gz  -C  ${DATA_DIR}/
    echo "Sorting node scores file"
    LANG=en_EN sort -k1,1 ${DATA_DIR}/freebase-nodes-scores.tsv > ${DATA_DIR}/freebase-nodes-scores.sort.tsv
    mv ${DATA_DIR}/freebase-nodes-scores.sort.tsv ${DATA_DIR}/freebase-nodes-scores.tsv
    rm -i -v freebase-nodes-scores.tsv.tar.gz
fi


ls -l ${DATA_DIR}/*

echo "Complete!"



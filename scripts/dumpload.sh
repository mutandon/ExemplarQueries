#!/bin/bash

OUTPUT='output-files'
GRAPH='freebase'
PERCENTILE=0.00001
TOPIC_ID=6773807984
TOPIC_MID="/m/topic"
SKIP_PATTERN='.*\t/user.*|.*\t/freebase/(?!domain_category).*|.*/usergroup/.*|.*\t/community/.*\t.*|.*\t/type/object/type\t.*|.*\t/type/domain/.*\t.*|.*\t/type/property/(?!expected_type|reverse_property)\b.*|.*\t/type/(user|content|attribution|extension|link|namespace|permission|reflect|em|karen|cfs|media).*|.*\t/common/(?!document|topic)\b.*|.*\t/common/document/(?!source_uri)\b.*|.*\t/common/topic/(description|image|webpage|properties|weblink|notable_for|article).*|.*\t/type/type/(?!domain|instance)\b.*|.*\t/dataworld/.*\t.*|.*\t/base/.*\t.*|.*\t/common/topic/alias\t.*|.*\t/type/property/.*\t.*|.*\t/common/topic/notable_types\t.*|.*\t/common/document.*\t.*|.*\t/type/object/key\t.*'


#Download freebase
freebase_dump="freebase-rdf-`date +%G-%m-%d`.gz"
wget http://commondatastorage.googleapis.com/freebase-public/rdf/freebase-rdf-latest.gz -O $freebase_dump

zcat $freebase_dump | cut -f 1-3 --output-delimiter=$'\t' | grep -E "(rdf.freebase.com/.*){3}" | tr -d '<' | tr -d '>' | sed "s/http:\/\/rdf\.freebase\.com\/ns//g" | tr '.' '/' | grep -v $TOPIC_MID | grep -Pv $SKIP_PATTERN |  gzip -c > ${freebase_dump%%.gz}-cleaned.gz
#rm -rf neo4j-data/*



#java -ea -jar -Dknowledge.config=knowledge-config.xml FreebaseDataLoad-1.0b.jar  none --gdb neo4j-data/prova.db/ --init
#java -ea -jar -Dknowledge.config=knowledge-config.xml FreebaseDataLoad-1.0b.jar  freebase-2014-04-13-00-00.gz --gdb neo4j-data/prova.db/ --load

### NOTE: Uncomment this line to run the java importer
#java -ea -jar -Dknowledge.config=knowledge-config.xml FreebaseDataLoad-1.0b.jar  none --gdb neo4j-data/prova.db/ --insert --isa


tsv2long $OUTPUT/freebase-relations.tsv $OUTPUT/relationship-names-relations.tsv $OUTPUT/freebase-rel.graph false
grep -v -E "/g/.+" $OUTPUT/freebase-isa.tsv | tsv2long $OUTPUT/relationship-names-isA.tsv $OUTPUT/freebase-isa.graph true

#Create the graph
cat $OUTPUT/freebase-rel.graph $OUTPUT/freebase-isa.graph > $OUTPUT/$GRAPH.graph
sort -n -k1 -t' ' $GRAPH.graph > $GRAPH-sout.graph
sort -n -k2 -t' ' $GRAPH.graph > $GRAPH-sin.graph

#Compute the edge-label frequencies
cut -f3 -d' ' $GRAPH.graph | sort | uniq -c | sort -nr | sed -E "s/^[ ]*([0-9]+) ([0-9]+)/\2 \1/g" > $GRAPH-label-frequencies.csv

#Compute big hubs
cut -f2 -d' ' $GRAPH-sin.graph > 1.tmp; cut -f1 -d' ' $GRAPH-sout.graph > 2.tmp
cat 1.tmp 2.tmp | sort | uniq -c | sort -nr | sed -E "s/^[ ]*([0-9]+) ([0-9]+)/\2 \1/g" > $GRAPH-node-frequencies.csv
rm 1.tmp 2.tmp
#Compute the number of lines needed to remove the big-hubs
lines=$( echo "(`wc -l $GRAPH-node-frequencies.csv | cut -f1 -d' '` * $PERCENTILE + 0.5) / 1" | bc )
head -n $lines $GRAPH-node-frequencies.csv | cut -f1 -d' ' > big-hubs.tsv
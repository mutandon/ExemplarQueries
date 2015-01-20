# ExemplarQueries

Exemplar queries is a new query paradigm. This library is able to use Freebase to process exemplar queries at run-time. 

This package contains a number of algorithms and optimizations to run exemplar queries. 

The test command to be used is "TestExemplar" it runs the complete flow. In order yo make it runs, freebase cleaned and the query files are needed. Query files are available in http://disi.unitn.it/~themis/exemplarquery/

**Requires** [ExecutionUtilities](https://github.com/mutandon/ExecutionUtilities "Execution Utilities") and [Grava](https://github.com/mutandon/Grava "Grava"). 




## Running Example

Running code and Example is not straightforward.

You need to have both the Execution Utilities and Grava.

Then you can start the DCMD CLI with

    java -Xmx100g -jar lib/ExecUtils-0.2.jar 

Then, inside the CLI, first you load the ExQ jar with the libs and then the Graph on top which you will run the experiments
 

    jar ExQ.jar -lib lib
    obj $graph BigMultigraphLoader -kb InputData/freebase/freebase

Finally you can run the Exemplar Query test with a command like

    exec TestExemplar -q InputData/queries/queries/my.query.file --graph $graph -kb InputData/freebase/freebase -lf InputData/freebase/freebase-label-frequencies.csv -l 0.5 -c 0.15  -topk 10  -t 0.005  -qout OutputData/quality-stats.csv -h InputData/freebase/big-hubs.tsv -k 2  -r 1
 

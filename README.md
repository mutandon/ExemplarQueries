# ExemplarQueries

Exemplar queries is a new query paradigm. 
This library is able to use Freebase & Yago to process exemplar queries at run-time. 

This package contains a number of algorithms and optimizations to run exemplar queries. 

The test command to be used is "TestExemplar" it runs the complete flow. In order to run it, the proper Freebase files cleaned and the query files are needed. 

Please see [The Exemplar Query project Page](https://disi.unitn.it/~lissandrini/exemplar.html) for more background.

**Requires** 
- [ExecutionUtilities](https://github.com/mutandon/ExecutionUtilities "Execution Utilities")
- [Grava](https://github.com/mutandon/Grava "Grava"). 

If you want to follow the easy installation steps, then it needs `bash`, `maven` for the `mvn` command, `git`, and `python 3` installed.

## Installation

1. Clone the repository, this will create the folder `ExemplarQueries
` with all the code inside
2. Run the Code Installation script `ExemplarQueries
/scripts/install.sh`
3. Run the script to get the data `ExemplarQueries
/scripts/get-data.sh`
4. Enter the deploy directory (called `ExQ` by default) and run the pre-processing script `cd ./ExQ && ./scripts/prepare.sh`
5. Get our queries: `curl https://disi.unitn.it/~lissandrini/files/queries-VLDB.zip -o /tmp/queries.zip && unzip /tmp/queries.zip  -d ./InputData/freebase/`



## Running Your Own Examples

Running code and Example is not straightforward.

You need to have both the Execution Utilities and Grava.

Then you can start the DCMD CLI with

    java -Xmx100g -jar lib/ExecUtils-0.2.jar 

Then, inside the CLI, first you load the ExQ jar with the libs and then the Graph on top which you will run the experiments


    jar ExQ.jar -lib lib
    obj $graph (BigMultigraphLoader -kb InputData/freebase/freebase)

**Note**: parentheses are important! 

Finally you can run the Exemplar Query test with a command like

    exec (TestExemplar -q InputData/queries/my.query.file --graph $graph -kb InputData/freebase/freebase -lf InputData/freebase/freebase-label-frequencies.csv -l 0.5 -c 0.15  -topk 10  -t 0.005  -qout OutputData/quality-stats.csv -h InputData/freebase/big-hubs.tsv   -r 1)


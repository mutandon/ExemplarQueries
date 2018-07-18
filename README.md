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

0.  Enter a project folder, here `./`, and run the following without entering sub-directories
1.  Clone this repository, this will create the folder `ExemplarQueries` with all the code inside
2.  Run the Code Installation script `./ExemplarQueries/scripts/install.sh`
3.  Run the script to get the data `./ExemplarQueries/scripts/get-data.sh`
4.  Enter the deploy directory (called `ExQ` by default) and run the pre-processing script `cd ./ExQ && ./scripts/prepare.sh`
5.  Get our queries: `curl https://disi.unitn.it/~lissandrini/files/queries-VLDB.zip -o /tmp/queries.zip && unzip /tmp/queries.zip  -d ./InputData/freebase/`
6.  You can run the test `./scripts/run.sh` or use the CLI in the `CommandUtilities`

## Running Your Own Examples with the CLI

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

These are useful because you can run different commands, while loading the data in memory only once, e.g., when you are debugging different versions of the code.

## Citing the work
Please cite us if you use the code in your project or publication
    
**Multi-Exemplar**

```bibtex
@inproceedings{lissandrini2018multi,
  title={Multi-Example Search in Rich Information Graphs},
  author={Lissandrini, Matteo and Mottin, Davide and Velegrakis, Yannis and Palpanas, Themis},
  booktitle={ICDE},
  year={2018}
}
```

**Exemplar queries journal**

```bibtex
@article{mottin2016exemplar,
  author={Mottin, Davide and Lissandrini, Matteo and Velegrakis, Yannis and Palpanas, Themis},
  title={Exemplar queries: a new way of searching},
  journal={VLDB J.},
  year={2016},
  pages={1--25},
  doi={10.1007/s00778-016-0429-2},
}
```

**Exemplar queries**

```bibtex
@article{mottin2014exemplar,
  title={Exemplar queries: Give me an example of what you need},
  author={Mottin, Davide and Lissandrini, Matteo and Velegrakis, Yannis and Palpanas, Themis},
  journal={PVLDB},
  volume={7},
  number={5},
  pages={365--376},
  year={2014},
  publisher={VLDB Endowment}
}
```

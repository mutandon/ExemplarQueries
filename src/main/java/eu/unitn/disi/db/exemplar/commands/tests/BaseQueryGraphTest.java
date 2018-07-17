/*
 * The MIT License
 *
 * Copyright 2014 Davide Mottin <mottin@disi.unitn.eu>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.unitn.disi.db.exemplar.commands.tests;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.DynamicInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import eu.unitn.disi.db.exemplar.commands.util.GraphFilesManager;
import eu.unitn.disi.db.command.exceptions.LoadException;
import eu.unitn.disi.db.exemplar.core.ranking.NodeScoring;
import eu.unitn.disi.db.exemplar.core.ranking.PrecomputedScoring;
import eu.unitn.disi.db.exemplar.core.ranking.ProximityBasedScoring;
import eu.unitn.disi.db.exemplar.core.ranking.RandomScoring;
import eu.unitn.disi.db.exemplar.core.ranking.StructuralSimilarityScoring;
import eu.unitn.disi.db.exemplar.core.ranking.UniformSimilarityScorng;
import eu.unitn.disi.db.exemplar.utils.NamesProvider;
import eu.unitn.disi.db.exemplar.utils.names.FreebaseNames;
import eu.unitn.disi.db.exemplar.utils.names.YagoNames;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public abstract class BaseQueryGraphTest extends Command {
    
    public static final String QUERY_FILE_EXTENSION = ".query";
    
    /**
     * Files  to load
     */
    protected String hubsFile;
    protected String labelFrequenciesFile;
    protected String labelNamesFile;
    protected String kbPath;    
    protected String dataPath;
    protected File inputDir;
    protected File outDir;
    
    /**
     * Query files
     */
    protected List<String> files;
    
    /**
     * Configurations
     */
    protected int repetitions;
    protected int cores;
    protected int limitComputation;
    protected boolean skipSave;
    protected int memoryLimit;
    protected int timeLimit;
    protected boolean mappable;

    
    
    /**
     * Precomputed Infos
     */
    protected BigMultigraph graph;
    protected List<Long> labels;
    
    protected Set<Long> bighubs;
    protected Map<Long, Double> labelInformativeness;
    protected Map<Long, Integer> labelFrequencies;
    protected Map<Long, Integer> labelsOrder;
    



    protected Runtime instance = Runtime.getRuntime();
    protected NamesProvider names;

    protected Ranking rankMethod;
    protected double rankThreshold;
    
    /**
     * Timers
     */
    protected StopWatch gWatch = new StopWatch();
    protected StopWatch stepWatch = new StopWatch();
    
    /**
     * Headers labels for CSV
     */
    public abstract static class Cols {

        public static final String QUERY = "QueryName";
        public static final String TTIME = "TotalTime";
        public static final String QEDGES = "Query Edges";
        public static final String QNODES = "Query Nodes";
        public static final String ANSWER = "Answer";
        public static final String OUTOFMEMORY = "Out of memory";
        public static final String INTERRUPTED = "Computation interrupted";

    }

    public enum Ranking {
        DEGREE("degree"),
        DIVERSITY("diversity"),
        STRUCTURAL("struct"),
        SKEWED_STRUCTURAL("skew"),
        UNIFORM("uniform"),
        PROXIMITY("proxi"),
        RANDOM("random"),
        PRECOMP("precomp");

        private final String type;
        private Path weightFile;
        private NodeScoring scoring = null;
        
        private Ranking(String type) {
            this.type = type;
            this.weightFile=null;
        }

        public void setWeightFile(Path wFile) {
            this.weightFile=wFile;
        }
        
        
        public boolean match(String match) {
            return match.toLowerCase().equals(this.type);
        }

        public NodeScoring getRanking() {            
            if(this.scoring == null){
                switch (this) {
                    case STRUCTURAL:
                        this.scoring = new StructuralSimilarityScoring();
                        break;
                    case UNIFORM:
                        this.scoring = new UniformSimilarityScorng();
                        break;
                    case PROXIMITY:
                        this.scoring = new ProximityBasedScoring();
                        break;
                    case PRECOMP:
                        this.scoring = new PrecomputedScoring(this.weightFile);
                        break;
                    case RANDOM:
                    default:
                        this.scoring = new RandomScoring();
                }
            }
            return this.scoring;
                    
        }

        @Override
        public String toString() {
            return type;
        }

    }
    
    
    
    
    /**
     * Find Graph files and try to load them
     *
     * @return list of Accessible QueryFiles paths
     */
    protected List<String> loadQueryFiles() {

        List<String> tmpFiles = new ArrayList<>();
        String tmp;
        if (this.inputDir.isDirectory()) {
            debug("Loading queries from directory " + this.inputDir.getAbsolutePath());

            File[] childFiles = this.inputDir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File child) {
                    return !child.isDirectory() && child.getName().endsWith(QUERY_FILE_EXTENSION);
                }
            });
            debug("Adding %s query graph: ", childFiles.length);
            for (File child : childFiles) {
                try {
                    tmp = child.getCanonicalPath();
                    tmpFiles.add(tmp);
                } catch (IOException | NullPointerException e) {
                    error("Query %s is not parsable! ", e, child.getAbsolutePath());
                    //error("Cause is " + e.getMessage());
                    //e.printStackTrace();
                }
            }
        } else {
            try {
                tmp = this.inputDir.getCanonicalPath();
                debug("Adding single graph: " + tmp);
                tmpFiles.add(tmp);
            } catch (IOException | NullPointerException e) {
                error("Query %s is not parsable! ", this.inputDir.getAbsolutePath());
                error("Cause is " + e.getMessage());
                //Logger.getLogger(TestExemplar.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return tmpFiles;
    }

    /**
     * Try to load query files
     *
     * @param files
     * @return each query string file associated with each parsed multigraph
     */
    protected List<Pair<String, Multigraph>> parseQueryGraphFiles(List<String> files) {
        // .. Each file is a query
        List<Pair<String, Multigraph>> queries = new ArrayList<>(files.size());
        Pair<String, Multigraph> out;
        for (String queryFile : files) {
            //1: TRY TO PARSE THE GRAPH
            debug("Now loading : " + queryFile);

            try {
                //------------------------------------------------------------------            
                GraphFilesManager gm = new GraphFilesManager(this.names);
                Multigraph queryGraph = gm.loadGraph(queryFile);
                if (isQueryMappable(queryGraph)) {
                    out = new Pair<>(queryFile, queryGraph);
                    queries.add(out);
                }
            } catch (ParseException | LoadException | NullPointerException e) {
                error("Query %s is not parsable! ", e, queryFile);
                //error("Cause is " + e.getMessage());
                //e.printStackTrace();
            }

        }

        return queries;

    }

    /**
     * Checks whether the query is in the knowledge graph
     *
     * @param queryGraph
     * @return
     */
    protected boolean isQueryMappable(Multigraph queryGraph) {
        if (!this.mappable) {
            for (Long node : queryGraph.vertexSet()) {
                if (!graph.containsVertex(node)) {
                    error("Query is not mappable in this Knowledge Graph, missing node %s", node);
                    return false;
                }
                if (queryGraph.outDegreeOf(node) > graph.outDegreeOf(node)) {
                    error("Query is not mappable in this Knowledge Graph, Graph node %s has  deg %s instead of %s ", node, graph.outDegreeOf(node), queryGraph.outDegreeOf(node));
                    Collection<Edge> edges = queryGraph.outgoingEdgesOf(node);

                    for (Edge e : edges) {
                        if (!graph.containsEdge(e.getSource(), e.getDestination())) {
                            error("Query is not mappable in this Knowledge Graph, missing edge %s", FreebaseNames.convertLongToMid(e.getSource()) + " -> " + FreebaseNames.convertLongToMid(e.getDestination()));
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     *
     * @return true if GC has been called
     */
    protected boolean checkMemory() {
        int gb = 1024 * 1024 * 1024;

        debug("Used memory %s over %s", (instance.totalMemory() - instance.freeMemory()) / gb, instance.totalMemory() / gb);
        if (instance.totalMemory() > instance.maxMemory() * 0.89 || ((1.0 * instance.totalMemory() - instance.freeMemory()) / instance.totalMemory() > 0.96)) {
            debug("System GC for once");
            System.gc();
            return true;
        }
        return false;
    }

    @Override
    protected String commandDescription() {
        return "Base command structure : should never be called!";
    }

    @CommandInput(
            consoleFormat = "-q",
            defaultValue = "",
            description = "input query",
            mandatory = true)
    public void setQuery(String input) throws ExecutionException {
        try {
            this.inputDir = new File(input);
        } catch (NullPointerException n) {
            throw new ExecutionException("Cannot read input directory or file %s", n, input);
        }
        if (!inputDir.exists() || !inputDir.canRead()) {
            throw new ExecutionException("Cannot read input directory or file %s", input);
        }
    }
    @CommandInput(
            consoleFormat = "-d",
            defaultValue = "InputData/freebase/",
            description = "path to the data files",
            mandatory = false)
    public void setDataPath(String data) {
        this.dataPath = data;
    }

    @CommandInput(
            consoleFormat = "-kb",
            defaultValue = "InputData/freebase/freebase",
            description = "path to the knowledgbase sin and sout files, just up to the prefix, like InputData/freebase ",
            mandatory = false)
    public void setKbPath(String kb) {
        this.kbPath = kb;
    }

    @DynamicInput(
            consoleFormat = "--graph",
            description = "multigraph used as a knowledge-base")
    public void setGraph(BigMultigraph graph) {
        this.graph = graph;
    }

    @CommandInput(
            consoleFormat = "-lf",
            defaultValue = "InputData/freebase/freebase-label-frequencies.csv",
            mandatory = false,
            description = "label frequency file formatted as 'labelid frequency'")
    public void setLabelFrequencies(String labelFrequencies) {
        this.labelFrequenciesFile = labelFrequencies;
    }

    @CommandInput(
            consoleFormat = "--labels",
            defaultValue = "", //InputData/freebase/fb-properties.txt
            mandatory = false,
            description = "label id translation mapping string to long")
    public void setLabelNamesFile(String labelNamesFile) throws ExecutionException {
        this.labelNamesFile = labelNamesFile;
        
        try {
            if (labelNamesFile.contains("fb") || labelNamesFile.contains("freebase")) {
                this.names = new FreebaseNames(labelNamesFile);
            } else if (labelNamesFile.contains("yago")) {
                /* YAGO takes a DIRECTORY 
                yago-labels.tsv
                yago-nodes-name.tsv
                 */
                this.names = new YagoNames(labelNamesFile);
            } else {                
                error("File for Node/Labels Names Not recognized: %s ", labelNamesFile);
                throw new ExecutionException("File for Node/Labels Names Not recognized: %s ", labelNamesFile);
            }

        } catch (IOException ex) {            
            error("Cannot read input directory or file %s", ex, labelNamesFile);
            throw new ExecutionException("Cannot read input directory or file %s", ex, labelNamesFile);
        } catch (Exception e){            
            error("Error while parsing %s", e, labelNamesFile);
            throw new ExecutionException("Unexpect Problem. Cannot read input directory or file %s", e, labelNamesFile);
        }
    }

    @CommandInput(
            consoleFormat = "-h",
            defaultValue = "InputData/nohubs.txt",
            mandatory = false,
            description = "node big hubs file")
    public void setHubs(String hubs) {
        this.hubsFile = hubs;
    }

    @CommandInput(
            consoleFormat = "-r",
            defaultValue = "1",
            mandatory = false,
            description = "number of repetitions per test")
    public void setRepetitions(int repetitions) {
        this.repetitions = repetitions;

    }

    @CommandInput(
            consoleFormat = "--cores",
            defaultValue = "24",
            mandatory = false,
            description = "core to use for parallelism")
    public void setNumCores(int cores) {
        this.cores = cores;
    }

    @CommandInput(
            consoleFormat = "-qout",
            defaultValue = "/tmp/null",
            description = "the name of the output destination",
            mandatory = false)
    public void setQueriesOut(String queriesOut) throws ExecutionException {
        this.outDir = new File(queriesOut);
        if (!outDir.exists() || !outDir.isDirectory()) {
            if (!outDir.mkdir()) {
                throw new ExecutionException("Cannot create the directory %s to store output", queriesOut);
            }
        }
    }

    @CommandInput(
            consoleFormat = "--mappable",
            defaultValue = "false",
            description = "assume all queries as mappable without making the check",
            mandatory = false)
    public void setMappable(boolean map) {
        this.mappable = map;
    }

    @CommandInput(
            consoleFormat = "--no-save",
            defaultValue = "false",
            mandatory = false,
            description = "discard results only compute time")
    public void setSkipSave(boolean skipSave) {
        this.skipSave = skipSave;
    }

    @CommandInput(
            consoleFormat = "--limit",
            defaultValue = "0",
            mandatory = false,
            description = "limit computation of isomorphism when too many results are found")
    public void setLimitComputation(int numLimit) {
        this.limitComputation = numLimit;

    }

    @CommandInput(
            consoleFormat = "--memory",
            defaultValue = "-1",
            mandatory = false,
            description = "memory (occupation) limit (megabytes) when reached stop the computation")
    public void setMemoryLimit(int memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    @CommandInput(
            consoleFormat = "--timeout",
            defaultValue = "-1",
            mandatory = false,
            description = "timeout limit (seconds) when reached stop the computation")
    public void setTimeLimit(int timeLimit) {
        this.timeLimit = timeLimit;
    }

    @CommandInput(
            consoleFormat = "--logLevel",
            defaultValue = "debug",
            description = "log4j level: debug, info, warn, error, fatal",
            mandatory = false)
    public void setLog(String lvl) {
        lvl = lvl.trim();
        if (lvl.equalsIgnoreCase("debug")) {
            this.logger.setLevel(Level.DEBUG);
        } else if (lvl.equalsIgnoreCase("info")) {
            this.logger.setLevel(Level.INFO);
        } else if (lvl.equalsIgnoreCase("warn")) {
            this.logger.setLevel(Level.WARN);
        } else if (lvl.equalsIgnoreCase("error")) {
            this.logger.setLevel(Level.ERROR);
        } else if (lvl.equalsIgnoreCase("fatal")) {
            this.logger.setLevel(Level.FATAL);
        } else {
            warn("ERROR LEVEL '" + lvl + "' NO RECOGNIZED! Setting: DEBUG");
            this.logger.setLevel(Level.DEBUG);
        }
    }

    
    @CommandInput(
            consoleFormat = "-rank",
            defaultValue = "none",
            mandatory = false,
            description = "Type of ranking function")
    public void setRanking(String ranking) {
        if(!ranking.equals("none")){            

            Path p = Paths.get(ranking);

            if (Files.exists(p) && Files.isRegularFile(p) && Files.isReadable(p)){
                this.rankMethod = Ranking.PRECOMP;
                this.rankMethod.setWeightFile(p);
                return;
            }
            for (Ranking r : Ranking.values()) {
                if( r.match(ranking)){
                    this.rankMethod = r;
                    return;
                }            
            }

            if(rankMethod == null){
                throw new IllegalArgumentException(ranking+" is not a valid ranking method");
            }
        }

    }
    
    @CommandInput(
            consoleFormat = "-rt",
            defaultValue = "0",// 0.0001
            mandatory = false,
            description = "minimum pruning threshold, 0 for all the graph")
    public void setRankThreshold(double threshold) {
        this.rankThreshold = threshold;
    }



    
}

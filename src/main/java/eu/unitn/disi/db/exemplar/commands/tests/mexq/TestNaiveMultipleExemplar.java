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
package eu.unitn.disi.db.exemplar.commands.tests.mexq;


import com.koloboke.collect.map.hash.HashLongIntMaps;
import com.koloboke.collect.set.hash.HashLongSets;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.DynamicInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.util.FileWriteOperation.Mode;
import eu.unitn.disi.db.command.util.stats.Statistics;
import eu.unitn.disi.db.command.util.stats.StatisticsCSVExporter;
import eu.unitn.disi.db.exemplar.commands.tests.BaseQueryGraphTest;
import eu.unitn.disi.db.exemplar.core.ExemplarAnswer;
import eu.unitn.disi.db.exemplar.core.algorithms.ConnectedComponents;
import eu.unitn.disi.db.exemplar.core.algorithms.ExemplarQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.GraphQueryEstimator;
import eu.unitn.disi.db.exemplar.core.algorithms.SampleExpansionRank;
import eu.unitn.disi.db.exemplar.core.storage.StorableTable;
import eu.unitn.disi.db.exemplar.core.storage.StorableTriple;
import eu.unitn.disi.db.exemplar.isomorphism.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.multiple.algorithms.MulExqStrict;
import eu.unitn.disi.db.exemplar.simulation.algorithms.ConnectedSimulatedQuerySearch;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.mutilities.data.CompoundSet;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Matteo Lissandrini  <ml@disi.unitn.eu>
 */
public class TestNaiveMultipleExemplar extends BaseQueryGraphTest {

    protected static final int STORED_TABLE_KEY_LENGTH = 3;
    
    protected boolean countOnly = false;

    
    
    protected int MAX_DEG = 11;
    protected String labelsDegreesPath;
    protected String labelsPairsPath;
    
    
    protected List<Map<Long, Integer>> labelDegreeFrequency;
    protected Map<Pair<Long, Long>, Integer> labelPairFrequency;
    protected Map<Long, Double> totalPairFreq;
    protected Map<Long, Integer> nodesHahMap;    
    protected List<Set<Long>> hashInvertedIndex;
    protected ArrayList<Set<Integer>> bitsets;
    //protected SetIndex stIndx;
    
    protected MatchMethod matchMethod;
    
    protected String globalStatsName = "global.stats.csv";

    protected boolean useRw = false;
    protected double minPprThreshold;
    protected double restartProb;

    
    

    protected int loadLimit = 6_000_000;

    public abstract static class Cols extends BaseQueryGraphTest.Cols {

        public static final String NUMSAMPLES = "Number of Samples";
        public static final String LOADTIME = "Time for loading the Graph";
        public static final String PRUNINGTIME = "Time for Table Pruning";
        public static final String COMPUTETABLETIME = "Time for Computing Tables";
        public static final String STORETABLETIME = "Time for Storing Tables";
        public static final String LOADTABLETIME = "Time for Loading Tables";
        public static final String EXEMPLARTIME = "Time For ISO Search (ms)";
        public static final String ISOCOMPS = "Number of ISO produced";
        public static final String PRGNODES = "Number of Nodes in Pruned Graph";
        public static final String PRGEDGES = "Number of Edges in Pruned Graph";
        public static final String NUMANSWERS = "Number of Answers";
        public static final String ESTANSWERS = "Estimated Answers";
        public static final String RWTIME = "Random Walk time";

    }

    
    public enum MatchMethod {
        ISO("iso"),
        SIM("sim"), // NOT SUPPORTED YET
        STRONG_SIM("strongsim");

        private final String type;

        private MatchMethod(String type) {
            this.type = type;
        }

        public boolean match(String match) {
            return match.toLowerCase().equals(this.type);
        }

        public ExemplarQuerySearch<ExemplarAnswer> getMethod() {            
            switch (this) {
                case ISO:
                    return new IsomorphicQuerySearch();
                case STRONG_SIM:
                    return new ConnectedSimulatedQuerySearch();
                case SIM:
                    throw  new UnsupportedOperationException("SIMULATION is not supported yet");
                default:
                    throw new IllegalArgumentException("Unrecognized method");
            }
        }
        
    }
    

    @Override
    protected void execute() throws ExecutionException {
        List<Pair<String, Multigraph>> multipleQueries;

        stepWatch.start();
        gWatch.start();
        // Load Stuff from Files
        files = loadQueryFiles();

        if (files.isEmpty()) {
            fatal("No Query file found!");
            throw new ExecutionException("No Query file found! for %s", this.inputDir.getAbsolutePath());
        }

        try {
            this.prepareData();
            System.gc();
            Runtime runtime = Runtime.getRuntime();                                        
            debug("Memory used is: %.2fMb, free memory: %.2fMb.", (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.), runtime.freeMemory()/(1024*1024.));
            
            info("Names from " + this.names.getName());            
            multipleQueries = this.parseQueryGraphFiles(files);
            info("Loaded data into main-memory in %dms", stepWatch.getElapsedTimeMillis());
        } catch (ParseException ex) {
            fatal("Query or Data  parsing failed", ex);
            throw new ExecutionException(ex);
        } catch (IOException ioex) {
            fatal("Unable to open some files: %s ", ioex, ioex.getMessage());
            throw new ExecutionException(ioex);
        } catch (NullPointerException ex) {
            fatal("Something wrong happened, message: %s", ex, ex.getMessage());
            ex.printStackTrace();
            throw new ExecutionException(ex);
        }

        /////  END LOADING  /////////////////////////////////////////////////////////////
        /// STATSS
        Statistics aggStat = new Statistics();
        aggStat.addStringField(Cols.QUERY);
        aggStat.addNumericField(Cols.NUMSAMPLES);
        aggStat.addNumericField(Cols.LOADTIME);
        aggStat.addNumericField(Cols.RWTIME);
        aggStat.addNumericField(Cols.PRGNODES);
        aggStat.addNumericField(Cols.PRGEDGES);       
        aggStat.addNumericField(Cols.COMPUTETABLETIME);
        aggStat.addNumericField(Cols.STORETABLETIME);
        aggStat.addNumericField(Cols.LOADTABLETIME);
        aggStat.addNumericField(Cols.EXEMPLARTIME);
        aggStat.addNumericField(Cols.ISOCOMPS);        
        aggStat.addNumericField(Cols.NUMANSWERS);
        aggStat.addNumericField(Cols.TTIME);
        aggStat.addNumericField(Cols.OUTOFMEMORY);
        aggStat.addNumericField(Cols.INTERRUPTED);

        Statistics stepsStats = new Statistics();
        stepsStats.addStringField(Cols.QUERY);                        
        stepsStats.addNumericField(Cols.EXEMPLARTIME);
        stepsStats.addNumericField(Cols.ESTANSWERS);
        stepsStats.addNumericField(Cols.NUMANSWERS);
        stepsStats.addNumericField(Cols.TTIME);

        String globalFileName = this.outDir + "/" + this.globalStatsName;
        StatisticsCSVExporter aggExp = new StatisticsCSVExporter(aggStat, globalFileName, Mode.OVERWRITE);

        String stepsFileName = this.outDir + "/" + "steps.stats.csv";
        StatisticsCSVExporter stepExp = new StatisticsCSVExporter(stepsStats, stepsFileName);

        stepWatch.reset();

        // ... Each List is a `Multiple` query
        long prevTime = 0;
        for (int repetition = 0; repetition < this.repetitions; repetition++) {
            // Iterate Execution through files
            int fileProgress = 0;
            for (Pair<String, Multigraph> mQuery : multipleQueries) {
                //VARIABLES SHARED FOR EACH ITERATION
                Multigraph prunedGraph;
                List<Multigraph> queries;
                ArrayList<Collection<Multigraph>> answers;

                //ALGORITHMS USED FOR EACH ITERATION
                SampleExpansionRank ppv;                
                ExemplarQuerySearch<ExemplarAnswer> searchAlgorithm;
                GraphQueryEstimator gqe;
                Map<Long, Double> nodesPPRValues;
                //???????????????????????????????????
                fileProgress++;
                boolean memoryExhausted = false; 
                boolean interrupted = false; 
                
                gWatch.reset();
                gWatch.start();                                
                Map<Long, Set<Long>> queryGraphMap = computeQueryGraphMap(mQuery.getSecond());                    
                try {

                    aggStat.addStringValue(Cols.QUERY, mQuery.getFirst());

                    queries = ConnectedComponents.splitGraph(mQuery.getSecond());
                    info("Query  %s contained %s queries", mQuery.getFirst(), queries.size());
                    answers = new ArrayList<>(queries.size());
                    aggStat.addNumericValue(Cols.NUMSAMPLES, queries.size());

                    //COMPUTE LARGE BIG NEIGHBORHOOD
                    stepWatch.reset();
                    stepWatch.start();
                    if (this.useRw) {
                        ppv = new SampleExpansionRank(this.graph);
                        HashSet<Long> qnodes = new HashSet<>(mQuery.getSecond().vertexSet());
                        ppv.setStartingNodes(qnodes);
                        ppv.setThreshold(this.minPprThreshold);
                        ppv.setRestartProbability(restartProb);
                        ppv.setMaxNumNodes(graph.numberOfNodes() / 10);
                        ppv.setLabelInformativeness(this.labelInformativeness);
                        ppv.setPriorityLabels(mQuery.getSecond().labelSet());
                        ppv.setKeepOnlyQueryEdges(false);
                        ppv.setHubs(bighubs);
                        ppv.compute();

                        //ppv.expandWithQueryEdges();
                        nodesPPRValues =  ppv.getPPRVector();
                        prunedGraph = ppv.getNeighborhood();
                        aggStat.addNumericValue(Cols.RWTIME, stepWatch.getElapsedTimeMillis());
                    } else {
                        prunedGraph = this.graph;
                        aggStat.addNumericValue(Cols.RWTIME, -1);
                        nodesPPRValues = null;
                    }
                    stepWatch.stop();
                                        
                    debug("Graph of %s Edges and %s Nodes in %s ms", prunedGraph.numberOfEdges(), prunedGraph.numberOfNodes(), stepWatch.getElapsedTimeMillis());
                    aggStat.addNumericValue(Cols.LOADTIME, gWatch.getElapsedTimeMillis());
                    aggStat.addNumericValue(Cols.PRGEDGES, prunedGraph.numberOfEdges());
                    aggStat.addNumericValue(Cols.PRGNODES, prunedGraph.numberOfNodes());

                    aggStat.addNumericValue(Cols.COMPUTETABLETIME, -1);
                    aggStat.addNumericValue(Cols.STORETABLETIME, -1);                    
                    aggStat.addNumericValue(Cols.LOADTABLETIME, -1);
                    
                    
                    int idx = 0;                    
                    long expectedMaxSize = 1;
                    int numIso = 0;
                    long isoTime = 0;
                    for (Multigraph queryGraph : queries) {
                        Collection<ExemplarAnswer> exemplarAnswers;
                        //Select the seed query
                        idx++;
                        stepsStats.addStringValue(Cols.QUERY, mQuery.getFirst() + "-" + idx);
                        stepWatch.reset();
                        stepWatch.start();

                        searchAlgorithm = this.matchMethod.getMethod();
                        searchAlgorithm.setMemoryLimit(memoryLimit);
                        searchAlgorithm.setTimeLimit(timeLimit);


                        // Find answers to the seed
                        Set<Long> whitelist = nodesPPRValues == null ? Collections.<Long>emptySet()  : nodesPPRValues.keySet();
                        exemplarAnswers = computeExemplar(queryGraph, prunedGraph, searchAlgorithm, stepsStats, queryGraphMap, whitelist);//, nodeHashClusters);
                        stepsStats.addNumericValue(Cols.TTIME, stepWatch.getElapsedTimeMillis());
                        debug("Found %s ExemplarAnswers to the query framgent %s", exemplarAnswers.size(), idx);
                        Collection<Multigraph> precompGraphs = new ArrayList<>(exemplarAnswers.size());
                        for (ExemplarAnswer precomp : exemplarAnswers) {
                            precompGraphs.add(precomp.buildMatchedGraph());
                        }

                        
                        
                        answers.add(precompGraphs);
                        numIso += exemplarAnswers.size();
                        expectedMaxSize *= exemplarAnswers.size();
                        stepWatch.stop();
                        isoTime += stepWatch.getElapsedTimeMillis();

                        gqe = new GraphQueryEstimator();
                        gqe.setLabelDegreeFrequency(labelDegreeFrequency);
                        gqe.setLabelFrequency(labelFrequencies);
                        gqe.setLabelPairFrequency(labelPairFrequency);
                        gqe.setTotalPairFreq(totalPairFreq);
                        gqe.setQuery(queryGraph);
                        gqe.compute();
                        stepsStats.addNumericValue(Cols.ESTANSWERS, gqe.getEstimation());
                        exemplarAnswers.clear();

                        if (searchAlgorithm.isInterrupted()) {
                            memoryExhausted = searchAlgorithm.isMemoryExhausted(); 
                            interrupted = true;
                            break; 
                        }
                        
                        
                    }

                    MulExqStrict mexq = new MulExqStrict();
                    mexq.setKeepOnlyCount(this.countOnly);
                    mexq.setExemplarAnswers(answers);
                    mexq.setMemoryLimit(memoryLimit);
                    mexq.setTimeLimit(timeLimit);
                    mexq.compute();
                    interrupted = mexq.isInterrupted() || interrupted; 
                    memoryExhausted = mexq.isMemoryExhausted() || memoryExhausted; 

                    //Collection<JointAnswer> mulAnswers = mexq.getMultiAnswers();

                    aggStat.addNumericValue(Cols.OUTOFMEMORY, memoryExhausted ? 1 : 0);
                    aggStat.addNumericValue(Cols.INTERRUPTED, interrupted ? 1 : 0);
                    aggStat.addNumericValue(Cols.ISOCOMPS, numIso);
                    aggStat.addNumericValue(Cols.EXEMPLARTIME, isoTime);
                    aggStat.addNumericValue(Cols.TTIME, gWatch.getElapsedTimeMillis());
                    aggStat.addNumericValue(Cols.NUMANSWERS, mexq.getNumMulAnswers());
                    aggExp.write();
                    gWatch.stop();
                    info("Found %s MulExQ in %s ms - Cartesian Product Size is %s", mexq.getNumMulAnswers(), mexq.getComputationTime(), expectedMaxSize);

                    aggExp.write();

//                    int limit = 5;
//                    String graphToExport;
//                    for (JointAnswer mulAnswer : mulAnswers) {
//                        toExport.addStringValue(Cols.QUERY, mQuery.getFirst());
//                        
//                        graphToExport = GraphFilesManager.getVIZString(mulAnswer.getGraph(), FreebaseConstants.getPropertiesToIdMap());
//                        toExport.addStringValue(Cols.ANSWER, graphToExport);
//                        if (limit-- < 1) {
//                            break;
//                        }
//                    }


                    long totTime = gWatch.getElapsedTimeMillis();
                    prevTime = prevTime == 0l ? totTime : (totTime * 3 / 4 + prevTime * 5 / 4) / 2;
                    long expectedSecs = (multipleQueries.size() - fileProgress) * (prevTime / 1000);
                    info("[%s/%s] Computed %d/%d in %s ms ETA %s secs", repetition + 1, this.repetitions, fileProgress, multipleQueries.size(), totTime, expectedSecs);

                    prunedGraph = null;
                    answers = null;
                    this.checkMemory();
                } catch (ExecutionException | NullPointerException e) {
                    error("ERROR for query %s", e, fileProgress);
                    e.printStackTrace();
                } catch (IOException ex) {
                    Logger.getLogger(TestNaiveMultipleExemplar.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }

        if (!stepsStats.isComplete()) {
            fatal("Step Stats is malformed");
        }
        if (!aggStat.isComplete()) {
            fatal("The Global stats file is malformed");
        }

        try {
            aggExp.write();
            debug("Saved globalPath %s", globalFileName);
            stepExp.write();
            debug("Saved stepsFileName %s", stepsFileName);
        } catch (IOException ex) {
            fatal("Unable to save statistics to File", ex);
            throw new ExecutionException(ex);
        }

    }

    
    /**
     * 
     * @param queryGraph
     * @return  Map from each node in the query to all nodes in the graph that can match it
     */
    protected Map<Long, Set<Long>> computeQueryGraphMap(Multigraph queryGraph) {
        //StoredInvertedIndexNeighborTables storedTables;
        Map<Long, Set<Long>> queryGraphMap = new HashMap<>();        
        Iterator<Edge> it;
        int numLabels = this.labelsOrder.size();
        Set<Integer> bb;
        Map<Long,Set<Integer>> bbs = new HashMap<>();
        Long lbl;
        StopWatch wa = new StopWatch();
        wa.start();
        for (Long node : queryGraph) {
            bb = new HashSet<>();
            for (int i = 0; i < 2; i++) {
                it = (i == 0 ? queryGraph.outgoingEdgesIteratorOf(node) : queryGraph.incomingEdgesIteratorOf(node));
                while (it.hasNext()) {
                    lbl = it.next().getLabel();
                    bb.add(i * numLabels + this.labelsOrder.get(lbl));
                }
            }
            bbs.put(node,bb);
            queryGraphMap.put(node, new CompoundSet<>());            
        }
        debug("Prepared  %s key bitset in %s ms", queryGraph.numberOfNodes(), wa.getElapsedTimeMillis());
        
        
        //bb = new BitSet(labelsOrder.size());
        int matched = 0;                
        int idx =0;        
        for(Set<Integer> container : this.bitsets){            
            for (Entry<Long, Set<Integer>> contained : bbs.entrySet()) {                                
                if (container.containsAll(contained.getValue())) {
                    matched++;
                    @SuppressWarnings("unchecked")
                    CompoundSet<Long> nodes = (CompoundSet<Long>)queryGraphMap.get(contained.getKey());                    
                    nodes.addAll(this.hashInvertedIndex.get(idx));                    
                    //estNodes += this.hashInvertedIndex.get(idx).size();                                        
                }
            }
            idx++;
        }
            
        ArrayList<Integer> matches = new ArrayList<>();
        for(Entry<Long, Set<Long>> maps : queryGraphMap.entrySet()){        
            matches.add(maps.getValue().size());
        }
                            
        wa.stop();
        debug("Loaded index for %s nodes in %s ms matching %s maps with %s size", queryGraphMap.size(), wa.getElapsedTimeMillis(), matched, matches);
        
        return queryGraphMap;
    }
        

    protected List<ExemplarAnswer> computeExemplar(Multigraph query, Multigraph prunedGraph,
            ExemplarQuerySearch<ExemplarAnswer> searchAlgorithm, Statistics stepStats, Map<Long, Set<Long>> queryGraphMap, Set<Long> whiteList) throws ExecutionException {//HashMap<Integer, HashSet<Long>> nodeHashClusters
        

        // Find Exemplars
        debug("Computing %s Exemplar Queries", this.matchMethod.name().toUpperCase());
        searchAlgorithm.setQueryToGraphMap(queryGraphMap);
        if(!whiteList.isEmpty()){
            searchAlgorithm.setWhiteList(whiteList);
        }
        searchAlgorithm.setQuery(query);
        searchAlgorithm.setStrictPruning(true);
        searchAlgorithm.setGraph(prunedGraph);
        searchAlgorithm.setNumThreads(this.cores);        
        //isoAlgorithm.setLimitedComputation(limitComputation);  // We do not limit computation
        searchAlgorithm.compute();
        debug("Computed %s Exemplar Queries in %s ms",  this.matchMethod.name().toUpperCase(),  searchAlgorithm.getComputationTime());
        int tt = 1;
        if (searchAlgorithm.isInterrupted()) {
            warn("SEARCH DIDN'T COMPLETE!");
            tt = -1;
        }
        stepStats.addNumericValue(Cols.EXEMPLARTIME, tt * searchAlgorithm.getComputationTime());       
        List<ExemplarAnswer> results = searchAlgorithm.getExemplarAnswers();
        stepStats.addNumericValue(Cols.NUMANSWERS, results.size());                
        return results;
    }

    @SuppressWarnings("unchecked")
    protected void prepareData() throws IOException, NullPointerException, InvalidClassException, ParseException {
        labelInformativeness = new HashMap<>(6500);
        labelFrequencies = new HashMap<>(6500);
        /**
         * CHECK NAME PROVIDER if(this.labelNamesFile!= null &&
         * !this.labelNamesFile.isEmpty()){ this.names = new
         * FreebaseNames(this.labelNamesFile); }
         *
         */

        info("Loading label frequencies from %s", labelFrequenciesFile);
        CollectionUtilities.readFileIntoMap(labelFrequenciesFile, " ", labelFrequencies, Long.class, Integer.class);

        double frequencies = 0.0;
        for (double freq : labelFrequencies.values()) {
            frequencies += freq;
        }

        bighubs = new HashSet<>();
        if (hubsFile != null && !hubsFile.isEmpty()) {
            CollectionUtilities.readFileIntoCollection(hubsFile, bighubs, Long.class);
        }

        double labelPrior;
        labels = new ArrayList<>(labelFrequencies.keySet());
        Collections.sort(labels);
        labelsOrder = new HashMap<>(labelFrequencies.size()*4/3);
        int idx =0;
        for (Long label : labels) {
            labelPrior = -Math.log(labelFrequencies.get(label) / (frequencies));
            labelInformativeness.put(label, labelPrior);
            labelsOrder.put(label, idx);
            idx++;
        }
                        

        if (graph == null) {
            graph = new BigMultigraph(this.kbPath + "-sin.graph", this.kbPath + "-sout.graph"/*, knowledgebaseFileSize*/);
            info("Loaded graph from file-path %s", this.kbPath);
        }
        if (graph == null) {
            throw new IllegalStateException("Null Knowledgebase!!");
        }
        info("Graph has %s nodes and %s edges", this.graph.numberOfNodes(), this.graph.numberOfEdges());

        this.labelsDegreesPath = Paths.get(this.dataPath, "label-degrees").toString();
        this.labelsPairsPath = Paths.get(this.dataPath, "label-pairs").toString();

        this.labelDegreeFrequency = new ArrayList<>(MAX_DEG);
        for (int i = 0; i < MAX_DEG; i++) {
            StorableTable lDegrees = new StorableTable(this.labelsDegreesPath, "labels-degree-" + i, "labels-degree-" + i + "-cardinalities");
            lDegrees.load();
            this.labelDegreeFrequency.add(lDegrees.getNodesMap());
        }

        StorableTriple lFreqs = new StorableTriple(this.labelsPairsPath, "label-pairs-list", "label-pairs-list-cardinalities", 10, true);
        lFreqs.load();
        this.labelPairFrequency = lFreqs.getNodesMap();
        lFreqs.clear();
        debug("Loaded %s Pair Frequencies", this.labelPairFrequency.size());

        this.totalPairFreq = new HashMap<>(6000);
        //TODO: Move out from here in preprocessing
        for (Map.Entry<Pair<Long, Long>, Integer> entry : this.labelPairFrequency.entrySet()) {
            Pair<Long, Long> key = entry.getKey();
            Double value = 1.0 * entry.getValue();
            totalPairFreq.merge(key.getFirst(), value, (Double t, Double u) -> (t + u));
            totalPairFreq.merge(key.getSecond(), value, (Double t, Double u) -> (t + u));
        }
        debug("Loaded %s Total Pair Frequencies", this.totalPairFreq.size());
        
        // BitSet Mapping of Label Neighborhood
        String nodeHashPath = this.dataPath + File.separator + "nodes-hash";
        FileInputStream fis = new FileInputStream(nodeHashPath + File.separator + "bitsets-l1.array");
        try (ObjectInputStream iis = new ObjectInputStream(fis)) {
            bitsets = (ArrayList<Set<Integer>>) iis.readObject();
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("Cannot deserialize bitsets!!");
        }

        // This is a simple 1 level index to retrieve bitsets faster
        //final int KEY=13; //23
        //this.stIndx = new SetIndex(bitsets, KEY);//23
        
        // Index: cluster id -> set of nodes
        hashInvertedIndex  = new ArrayList<>(bitsets.size());
        int iter = bitsets.size();
        for(int i=0; i<iter; i++){                                
            hashInvertedIndex.add(HashLongSets.newUpdatableSet());//new HashSet<>(1_000));;            
        }
        
        if (nodesHahMap == null) {
            StorableTable stp = new StorableTable(nodeHashPath, "node-hashing-id-l1", "node-hashing-values-l1");
            stp.load();
            nodesHahMap = HashLongIntMaps.newUpdatableMap(stp.getNodes().size()); //new HashMap<>(stp.getNodes().size()*4/3 +1); //;
            // Each node is mapped to the integer ID of its BitSet
            for (Pair<Long, Integer> p : stp) {
                // We retriev the set of nodes that share the same BitSet signature/ID
                Set<Long> nodes = hashInvertedIndex.get(p.getSecond());                
                // We add the current node to the set of nodes with the same signature
                nodes.add(p.getFirst());       
                
                //We update the index Node ->cluster ID
                nodesHahMap.put(p.getFirst(), p.getSecond());
                
            }
            debug("Loaded %s Node Hashes", this.nodesHahMap.size());
            stp.clear();
        } else {
            debug("Node Hashes already loaded!");
        }
        if (nodesHahMap == null) {
            throw new IllegalStateException("Null HashIndex!!");
        }

        
        
        
                
        debug("Test ranking setup %s ", this.rankMethod.toString());
        this.rankMethod.getRanking();       
    }
       

    @CommandInput(
            consoleFormat = "--count",
            defaultValue = "false",
            description = "just keep counts, may count duplicates",
            mandatory = false)
    public void setCountOnly(boolean just) {
        this.countOnly = just;
    }

    @CommandInput(
            consoleFormat = "-ppv",
            defaultValue = "0",// 0.0001
            mandatory = false,
            description = "minimum pruning threshold, 0 for all the graph")
    public void setPprThreshold(double threshold) {
        this.minPprThreshold = threshold;
        if (threshold > 0) {
            this.useRw = true;
        }
    }

    @CommandInput(
            consoleFormat = "-c",
            defaultValue = "0.15",
            mandatory = false,
            description = "restart probability")
    public void setDispersion(double c) {
        this.restartProb = c;
    }

    @CommandInput(
            consoleFormat = "-method",
            defaultValue = "iso",
            mandatory = false,
            description = "Type of matching function: isomorphism, strong simulation are enabled")
    public void setMatchMethod(String method) {

        for (MatchMethod m : MatchMethod.values()) {
            if (m.match(method)) {
                this.matchMethod = m;
                return;
            }
        }

        if (rankMethod == null) {
            throw new IllegalArgumentException(method + " is not a valid matching method");
        }

    }

    @DynamicInput(
            consoleFormat = "--hash",
            description = "hash index")
    public void setHashIndex(HashMap<Long, Integer> hash) {
        this.nodesHahMap = hash;
    }

}

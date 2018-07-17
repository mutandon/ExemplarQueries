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
package eu.unitn.disi.db.exemplar.commands.tests.exq;

import com.koloboke.collect.map.hash.HashLongIntMaps;
import com.koloboke.collect.set.hash.HashLongSets;
import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.exemplar.commands.tests.BaseQueryGraphTest;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.command.util.stats.Statistics;
import eu.unitn.disi.db.command.util.stats.StatisticsCSVExporter;
import eu.unitn.disi.db.exemplar.core.VectorSimilarities;
import eu.unitn.disi.db.exemplar.core.ExemplarAnswer;
import eu.unitn.disi.db.exemplar.isomorphism.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.SampleExpansionRank;
import eu.unitn.disi.db.exemplar.core.storage.StorableTable;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.data.CompoundSet;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TestExemplar extends BaseQueryGraphTest {
        
    /**
     * Configurations
     */
    protected int neighborSize;
    protected int topK;    
    protected double threshold;
    protected double restartProb;
    protected double lambda;
    protected boolean skipPruning;
    protected boolean skipNeighborhood;
               

    /**
     * Node Index
     */
    protected ArrayList<Set<Integer>> bitsets; 
    protected List<Set<Long>> hashInvertedIndex;
    protected Map<Long, Integer> nodesHahMap;    
        
    
    /**
     *
     */
    public abstract static class Cols {
        /**
         * "QueryName"
         */
        public static final String QUERY = "QueryName";
        public static final String NSIZE_E = "Neighborhood Edges";
        public static final String NSIZE_V = "Neighborhood Vertex";        
        public static final String PSIZE_V = "PrunedSize Vertex";
        public static final String SOLUTIONS = "Num of Solution";
        public static final String NTIME = "Neighborhood Time";        
        public static final String PTIME = "Pruning Time";
        public static final String RTIME = "Ranking Time";
        public static final String STIME = "SearchTime";

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

        try {
            
            Statistics stat;

            // Agregate statistics fo this complete Run
            Statistics aggStat = new Statistics();
            aggStat.addStringField(Cols.QUERY);
            aggStat.addNumericField(Cols.NSIZE_E);
            aggStat.addNumericField(Cols.NSIZE_V);            
            aggStat.addNumericField(Cols.PSIZE_V);
            aggStat.addNumericField(Cols.SOLUTIONS);
            aggStat.addNumericField(Cols.NTIME);            
            aggStat.addNumericField(Cols.PTIME);
            aggStat.addNumericField(Cols.RTIME);
            aggStat.addNumericField(Cols.STIME);

            // Load Stuff from Files
            
            // Iterate through files
            int fileProgress = 0;
            gWatch.reset();
            if (!files.isEmpty()) {
                // .. Each file is a query
                for (Pair<String, Multigraph> mQuery : multipleQueries) {
                    // ALGOS
                    SampleExpansionRank ppv = new SampleExpansionRank(graph);

                    //DATA
                    String queryFile;
                    Multigraph queryGraph;
                    Collection<Long> neighbourStartingNodes = new LinkedHashSet<>();
                    Collection<Long> queryEdgeLabels = new HashSet<>();
                    

                    //VARIABLES
                    boolean processError = false; // This is set to true if prunign goes south

                    // Prepare Single query Stats
                    stat = new Statistics();
                    stat.addStringField(Cols.QUERY);
                    stat.addNumericField(Cols.NSIZE_E);
                    stat.addNumericField(Cols.NSIZE_V);
                    stat.addNumericField(Cols.PSIZE_V);
                    stat.addNumericField(Cols.SOLUTIONS);
                    stat.addNumericField(Cols.NTIME);
                    stat.addNumericField(Cols.PTIME);
                    stat.addNumericField(Cols.RTIME);
                    stat.addNumericField(Cols.STIME);


                    //1: THE EXEMPLAR GRAPH
                    queryFile=mQuery.getFirst();
                    debug("Now testing : " + queryFile);
                    queryGraph = mQuery.getSecond();
                    

                    // The nodes from which we start neighrborhood exploration
                    neighbourStartingNodes.addAll(queryGraph.vertexSet());


                    // The edge labels for priortizing exploration
                    for (Edge edge : queryGraph.edgeSet()) {
                        queryEdgeLabels.add(edge.getLabel());
                    }


                    // Multiple Iterations for the same query
                    for (int experimentIterations = 0; experimentIterations < repetitions; experimentIterations++) {                    
                        debug("Repetition %d", experimentIterations);
                        if(processError){
                            error("Skipping iteration, pruning has gone wrong");
                            break;
                        }



                        try {
                            StopWatch watch = new StopWatch();

                            Multigraph neighborhood ;                            

                            stat.addStringValue(Cols.QUERY, queryFile);
                            Map<Long, Set<Long>> queryGraphMap;


                            watch.start();
                            //2. PRUNE THE SPACE
                            //2.1: PAGE RANK PRUNING
                            watch.reset();
                            if (!skipNeighborhood) {

                                ppv.setStartingNodes(neighbourStartingNodes);
                                ppv.setThreshold(threshold);
                                ppv.setRestartProbability(restartProb);
                                ppv.setMaxNumNodes(this.neighborSize);
                                ppv.setLabelInformativeness(labelInformativeness);
                                ppv.setPriorityLabels(queryEdgeLabels);
                                ppv.setKeepOnlyQueryEdges(true);
                                ppv.setHubs(bighubs);
                                ppv.compute();
                                neighborhood = ppv.getNeighborhood();

                                debug("Time to get the most important neighbors: %dms", watch.getElapsedTimeMillis());
                                stat.addNumericValue(Cols.NTIME,watch.getElapsedTimeMillis());

                                debug("Neighbors contains %d edges and %d vertexes", neighborhood.numberOfEdges(), neighborhood.vertexSet().size());
                                stat.addNumericValue(Cols.NSIZE_E, neighborhood.numberOfEdges());
                                stat.addNumericValue(Cols.NSIZE_V, neighborhood.numberOfNodes());


                            } else {
                                debug("Skipping neighborhood pruning!");
                                neighborhood = graph;
                                
                                //debug("Time to get the most important neighbors: %dms", watch.getElapsedTimeMillis());
                                //debug("NaiveNeighbors contains %d edges and %d vertexes", neighborhood.edgeSet().size(), neighborhood.vertexSet().size());
                                stat.addNumericValue(Cols.NTIME,-1);
                                stat.addNumericValue(Cols.NSIZE_E, graph.numberOfEdges());
                                stat.addNumericValue(Cols.NSIZE_V, graph.numberOfNodes());

                            }
                            
                            
                            //2.3: TABLE PRUNING
                            if (!skipPruning) {                                
                                debug("Preparing for Pruning with neighbors tables");
                                watch.reset();                                
                                //2.5: COMPUTE SIMULATION MAPPING
                                queryGraphMap = computeQueryGraphMap(queryGraph);                    
                                
                                                                                                                              
                                debug("Time to compute the graph mapping to prune the graph: %dms, found", watch.getElapsedTimeMillis());
                                stat.addNumericValue(Cols.PTIME,watch.getElapsedTimeMillis());
                                int numNodes = 0;
                                for(Set<Long> n : queryGraphMap.values()){
                                    numNodes += n.size();
                                }
                                stat.addNumericValue(Cols.PSIZE_V, numNodes);
                            } else {
                                queryGraphMap = new HashMap<>();
                                stat.addNumericValue(Cols.PTIME,-1);
                                stat.addNumericValue(Cols.PSIZE_V,-1);                             
                            }

                            //3: SEARCH EXEMPLAR ANSWERS
                            watch.reset();
                            HashSet<ExemplarAnswer> exemplarAnswersUnique;
                            List<ExemplarAnswer> exemplarAnswers;

                            IsomorphicQuerySearch isoAlgorithm = new IsomorphicQuerySearch();

                            isoAlgorithm.setQuery(queryGraph);                                                                                    
                            isoAlgorithm.setQueryToGraphMap(queryGraphMap);
                            isoAlgorithm.setNumThreads(this.cores);
                            isoAlgorithm.setLimitedComputation(this.limitComputation);
                            isoAlgorithm.setSkipSave(this.skipSave);
                            isoAlgorithm.setMemoryLimit(this.memoryLimit);
                            isoAlgorithm.setGraph(neighborhood);
                            isoAlgorithm.compute();

                            exemplarAnswers = isoAlgorithm.getExemplarAnswers();
                            stat.addNumericValue(Cols.STIME, watch.getElapsedTimeMillis());

                            exemplarAnswersUnique = new HashSet<>(exemplarAnswers);
                            stat.addNumericValue(Cols.SOLUTIONS, exemplarAnswersUnique.size());
                            debug("Found %d related queries of which uniques are %d", exemplarAnswers.size(), exemplarAnswersUnique.size());


                            //4: RANK QUERIES
                            if (experimentIterations == 0 && topK > 0) {
                                int a;
                                long rankingTime = 0l;
                                double pop, similarity;
                                
                                Map<Long, Double> popularities;
                                TreeSet<ExemplarAnswer> orderedQueriesIntersect = new TreeSet<>();
                                TreeSet<ExemplarAnswer> orderedQueriesNoIntersect = new TreeSet<>();


                                info("Ordering related queries");

                                watch.reset();

                                if(skipNeighborhood){
                                    // IF we have skipped the neighborhood we now need to
                                    // comput the ppr
                                    info("Recompute Neighborhood and vectors for ranking");
                                    ppv.setStartingNodes(neighbourStartingNodes);
                                    ppv.setThreshold(threshold);
                                    ppv.setRestartProbability(restartProb);
                                    ppv.setMaxNumNodes(this.neighborSize);
                                    ppv.setLabelInformativeness(labelInformativeness);
                                    ppv.setPriorityLabels(queryEdgeLabels);
                                    ppv.setKeepOnlyQueryEdges(true);
                                    ppv.setHubs(bighubs);
                                    ppv.setKeepOnlyQueryEdges(false);
                                    ppv.compute();
                                }
                               
                                popularities = new HashMap<>(ppv.getPPRVector());
                                
                                rankingTime += watch.getElapsedTimeMillis();                                
                                watch.reset();

                                for (ExemplarAnswer relQuery : exemplarAnswersUnique) {
                                    int intersections = 1;

                                    Set<Long> mappedNodes = relQuery.getUsedNodes();
                                    for (Long q : queryGraph.vertexSet()) {
                                        if (mappedNodes.contains(q)) {
                                            intersections++;
                                        }
                                    }
                                    intersections *= intersections;
                                    for (Long q : neighbourStartingNodes) {
                                        similarity = 0;
                                        pop = 0;
                                        for( Long n1 : relQuery.mapOf(q)){

                                            if (popularities.containsKey(n1)) {
                                                pop += popularities.get(n1);
                                            }

                                            similarity += score(this.bitsets.get(this.nodesHahMap.get(q)), this.bitsets.get(this.nodesHahMap.get(n1)));

                                        }

                                        relQuery.addWeight(q, (((lambda * similarity ) + (1 - lambda) * pop)/relQuery.mapOf(q).size()));
                                    }
                                    if(intersections == 1){
                                       orderedQueriesNoIntersect.add(relQuery);
                                    } else {
                                       orderedQueriesIntersect.add(relQuery);
                                    }
                                }
                                rankingTime += watch.getElapsedTimeMillis();
                                info("Ranked related queries in %dms", watch.getElapsedTimeMillis());
                                stat.addNumericValue(Cols.RTIME, rankingTime);

                                //9: PRINT RELATED
                                info("Top-%d related queries", topK);


                                Statistics rank = new Statistics();
                                rank.addNumericField("Position");
                                rank.addNumericField("Weight");
                                rank.addStringField("Query");
                                rank.addNumericField("Intersect");

                                for(int type = 0;  type<=1 ;type ++ ){
                                    NavigableSet<ExemplarAnswer> sortedSet = (type == 0 ? orderedQueriesNoIntersect : orderedQueriesIntersect ).descendingSet();
                                    a=0;
                                    for (ExemplarAnswer rQuery : sortedSet) {
                                        a++;
                                        if (a <= topK || a == sortedSet.size() ) {
                                            rank.addNumericValue("Position", a);
                                            rank.addNumericValue("Intersect", type);
                                            rank.addNumericValue("Weight", rQuery.getTotalWeight());

                                            System.out.println(rQuery.getTotalWeight());
                                            
                                            String s = String.format("[Q%d,value=%f]", a, rQuery.getTotalWeight());
                                            String q = "";
                                            Set<Edge> mappedEdges = rQuery.getUsedEdges();

                                            int tempEdgeCnt = 0;
                                            for (Edge edge : mappedEdges ) {
                                                if(tempEdgeCnt>0){
                                                    q+= " | ";
                                                }
                                                //FreebaseConstants.convertLongToMid(
                                                q += (edge.getSource())
                                                        + "->"
                                                        + ( edge.getDestination());
                                                tempEdgeCnt++;
                                            }
                                            rank.addStringValue("Query", q);
                                            //debug(s + q);
                                        }
                                    }
                                }

                                StatisticsCSVExporter rankxp = new StatisticsCSVExporter(rank, this.outDir +  "/" +(new File(queryFile)).getName() + ".top-"+ topK +".csv");                                
                                rankxp.write();
                            //END IF i==0
                            }  else {
                                stat.addNumericValue(Cols.RTIME, -1);
                            }


                        } catch (AlgorithmExecutionException ex) {
                            error("ERROR WHILE COMPUTING: %s", ex.getMessage());
                            ex.printStackTrace();
                            processError = true;
                        }

                    } //END FOR REPETITIONS


                    StatisticsCSVExporter xp = new StatisticsCSVExporter(stat,this.outDir +  "/" +(new File(queryFile)).getName() + ".stats.csv");                    
                    xp.write();


                    aggStat.addStringValue(Cols.QUERY, (new File(queryFile)).getName());
                    aggStat.addNumericValue(Cols.NSIZE_E,stat.getAverage(Cols.NSIZE_E) );
                    aggStat.addNumericValue(Cols.NSIZE_V, stat.getAverage(Cols.NSIZE_V));
                    aggStat.addNumericValue(Cols.PSIZE_V, stat.getAverage(Cols.PSIZE_V));
                    aggStat.addNumericValue(Cols.SOLUTIONS, stat.getAverage(Cols.SOLUTIONS));
                    aggStat.addNumericValue(Cols.NTIME, stat.getAverage(Cols.NTIME));
                    aggStat.addNumericValue(Cols.PTIME, stat.getAverage(Cols.PTIME));
                    aggStat.addNumericValue(Cols.RTIME, stat.getMax(Cols.RTIME));
                    aggStat.addNumericValue(Cols.STIME, stat.getAverage(Cols.STIME));



                    fileProgress++;
                    info("STATUS:   Parsed %s out of %s Queries in  %s secs  expected remaing time %s secs", fileProgress, files.size(), gWatch.getElapsedTimeSecs(), gWatch.getElapsedTimeSecs()*(files.size()-fileProgress)/(fileProgress*1.0)    );
                    if(fileProgress%20 == 0){
                        //FREE MAIN MEMORY (hopefully)
                        debug("GC");
                        System.gc();
                        debug("GC Done");

                    }

                } //END FOR FILES


                StatisticsCSVExporter aggxp = new StatisticsCSVExporter(aggStat, this.outDir +  "/aggregate.stats.csv");                
                aggxp.write();

            } //END CHECK FILE EMPTY
        } catch (IOException ioex) {
            fatal("Unable to open files: ", ioex);
            throw new ExecutionException(ioex);
        } catch (NullPointerException | IllegalStateException ex) {
            fatal("Something wrong happened, message: %s", ex.getMessage());
            ex.printStackTrace();
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
                        
            for (Map.Entry<Long, Set<Integer>> contained : bbs.entrySet()) {                                
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
        for(Map.Entry<Long, Set<Long>> maps : queryGraphMap.entrySet()){        
            matches.add(maps.getValue().size());
        }
                            
        wa.stop();
        debug("Loaded index for %s nodes in %s ms matching %s maps with %s size", queryGraphMap.size(), wa.getElapsedTimeMillis(), matched, matches);
        
        return queryGraphMap;
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

    /**
     * See Equation 5 in the paper
     * @param vector
     * @param levelNodes
     * @param level
     */
    protected void updateVector(Map<Long, Double> vector, Set<Integer> levelNodes,  int level) {
        
        Double value;
        Long label;
        int sqLevel = level * level;

        for (Integer labelIdx : levelNodes) {            
            label = this.labels.get(labelIdx);
            value = vector.getOrDefault(label, 0.0);
            value += (labelInformativeness.get(label)) / sqLevel;
            vector.put(label, value);
        }
    }

    /**
     * Structural similarity between two nodes
     * @param node1
     * @param node2
     * @return
     */
    protected double score(Set<Integer> node1, Set<Integer> node2) {
        
        Map<Long, Double> vector1 = new HashMap<>(), vector2 = new HashMap<>();
        
        updateVector(vector1, node1, 1);
        updateVector(vector2, node2, 1);

        return VectorSimilarities.cosine(vector1, vector2, true);
    }


    
    


    @Override
    protected String commandDescription() {
        return "Compute exemplar queries on Freebase";
    }

    @CommandInput(
            consoleFormat = "-p",
            defaultValue = "false",
            mandatory = false,
            description = "avoid using the pruning algorithm")
    public void setUsePruning(boolean usePruning) {
        this.skipPruning = usePruning;
    }

    @CommandInput(
            consoleFormat = "-n",
            defaultValue = "false",
            mandatory = false,
            description = "avoid using the neighborhood algorithm")
    public void setUseNeighborhood(boolean useNeighborhood) {
        this.skipNeighborhood = useNeighborhood;
    }

    @CommandInput(
            consoleFormat = "-topk",
            defaultValue = "0",
            mandatory = false,
            description = "number of exemplar queries to output (top-k), when 0 skips ranking")
    public void setTopK(int topK) {
        this.topK = topK;
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
            consoleFormat = "-s",
            defaultValue = "0",
            mandatory = false,
            description = "neighbor size")
    public void setNeighborSize(int size) {
        //IF ZERO IT WON'T TRIGGER
        this.neighborSize = size;
    }

    @CommandInput(
            consoleFormat = "-t",
            defaultValue = "1",
            mandatory = false,
            description = "pruning threshold")
    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }


    @CommandInput(
            consoleFormat = "-l",
            defaultValue = "0.5",
            description = "lambda used in the scoring function",
            mandatory = false)
    public void setLambda(double lambda) {
        this.lambda = lambda;
    }


    

}




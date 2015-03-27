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
package eu.unitn.disi.db.exemplar.commands;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.DynamicInput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.command.util.stats.Statistics;
import eu.unitn.disi.db.command.util.stats.StatisticsCSVExporter;
import eu.unitn.disi.db.exemplar.core.VectorSimilarities;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.PersonalizedPageRank;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.IsomorphicQuerySearch;
import eu.unitn.disi.db.exemplar.exceptions.LoadException;
import eu.unitn.disi.db.exemplar.freebase.FreebaseConstants;
import eu.unitn.disi.db.exemplar.commands.util.GraphFilesManager;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NeighborTables;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TestExemplar extends Command {

    protected BigMultigraph graph;
    protected String query;
    protected String hubsFile;
    protected int neighborSize;
    protected int topK;
    protected int repetitions;
    protected double threshold;
    protected double restartProb;
    protected double lambda;
    protected boolean skipPruning;
    protected boolean skipNeighborhood;
    protected boolean limitComputation;
    protected boolean skipSave;
    protected String labelFrequenciesFile;
    protected String kbPath;
    protected int k;
    protected String queriesOut;
    protected Set<Long> bighubs;
    protected HashMap<Long, Double> informativeness;
    protected int cores;

    public static final int TABLE_CORES = 12;

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
        public static final String PSIZE_E = "PrunedSize Edges";
        public static final String PSIZE_V = "PrunedSize Vertex";
        public static final String SOLUTIONS = "Num of Solution";
        public static final String NTIME = "Neighborhood Time";
        public static final String TTIME = "Table Time";
        public static final String PTIME = "Pruning Time";
        public static final String STIME = "SearchTime";

    }

    @Override
    protected void execute() throws ExecutionException {
        StopWatch globalWatch;
        List<String> files = new ArrayList<>();

        File queriesOutDir = new File(queriesOut);
        if (!queriesOutDir.exists() || !queriesOutDir.isDirectory()) {
            if (!queriesOutDir.mkdir()) {
                throw new ExecutionException("Cannot create the directory %s to store queries", queriesOut);
            }
        }

        try {
            globalWatch = new StopWatch();
            Statistics stat;

            // Agregate statistics fo this complete Run
            Statistics aggStat = new Statistics();
            aggStat.addStringField(Cols.QUERY);
            aggStat.addNumericField(Cols.NSIZE_E);
            aggStat.addNumericField(Cols.NSIZE_V);
            aggStat.addNumericField(Cols.PSIZE_E);
            aggStat.addNumericField(Cols.PSIZE_V);
            aggStat.addNumericField(Cols.SOLUTIONS);
            aggStat.addNumericField(Cols.NTIME);
            aggStat.addNumericField(Cols.TTIME);
            aggStat.addNumericField(Cols.PTIME);
            aggStat.addNumericField(Cols.STIME);

            // Load Stuff from Files
            files = loadQueryFiles(this.query);

            informativeness = new HashMap<>();
            info("Loading label frequenies from %s", labelFrequenciesFile);
            Utilities.readFileIntoMap(labelFrequenciesFile, " ", informativeness, Long.class, Double.class);

            double frequencies = 0.0;
            for (double freq : informativeness.values()) {
                frequencies += freq;
            }

            bighubs = new HashSet<>();
            if (hubsFile != null && !hubsFile.isEmpty()) {
                Utilities.readFileIntoCollection(hubsFile, bighubs, Long.class);
            }


            double labelPrior = 0;
            Set<Long> labels = informativeness.keySet();
            for (Long label : labels) {
                labelPrior = -Math.log(informativeness.get(label) / (frequencies));
                informativeness.put(label, labelPrior);
            }

            globalWatch.start();
            if (graph == null) {
                graph = new BigMultigraph(this.kbPath + "-sin.graph", this.kbPath + "-sout.graph"/*, knowledgebaseFileSize*/);
                info("Loaded graph from file-path %s", this.kbPath);
            }
            if (graph == null) {
                throw new IllegalStateException("Null Knowledgebase!!");
            }


            info("Loaded freebase into main-memory in %dms", globalWatch.getElapsedTimeMillis());
            //CLEANS
            debug("GC");
            System.gc();
            debug("GC Done");

            // Iterate through files
            int fileProgress = 0;
            globalWatch.reset();
            if (!files.isEmpty()) {
                // .. Each file is a query
                for (String queryFile : files) {
                    // ALGOS
                    PersonalizedPageRank ppv = new PersonalizedPageRank(graph);
                    ComputeGraphNeighbors tableAlgorithm = new ComputeGraphNeighbors();
                    PruningAlgorithm pruningAlgorithm = new PruningAlgorithm();

                    //DATA
                    Multigraph queryGraph;
                    Collection<Long> neighbourStartingNodes = new HashSet<>();
                    Collection<Long> queryEdgeLabels = new HashSet<>();
                    NeighborTables graphTables = null, queryTables = null;

                    //VARIABLES
                    boolean processError = false; // This is set to true if prunign goes south

                    // Prepare Single query Stats
                    stat = new Statistics();
                    stat.addStringField(Cols.QUERY);
                    stat.addNumericField(Cols.NSIZE_E);
                    stat.addNumericField(Cols.NSIZE_V);
                    stat.addNumericField(Cols.PSIZE_E);
                    stat.addNumericField(Cols.PSIZE_V);
                    stat.addNumericField(Cols.SOLUTIONS);
                    stat.addNumericField(Cols.NTIME);
                    stat.addNumericField(Cols.TTIME);
                    stat.addNumericField(Cols.PTIME);
                    stat.addNumericField(Cols.STIME);


                    //1: TRY TO PARSE THE GRAPH
                    debug("Now loading : " + queryFile);
                    queryGraph = GraphFilesManager.loadGraph(queryFile);

                    if(!isQueryMappable(queryGraph)){
                        stat.addStringValue(Cols.QUERY, "ERROR");
                        stat.addNumericValue(Cols.NSIZE_E, -1);
                        stat.addNumericValue(Cols.NSIZE_V, -1);
                        stat.addNumericValue(Cols.PSIZE_E, -1);
                        stat.addNumericValue(Cols.PSIZE_V, -1);
                        stat.addNumericValue(Cols.SOLUTIONS, -1);
                        stat.addNumericValue(Cols.NTIME, -1);
                        stat.addNumericValue(Cols.TTIME, -1);
                        stat.addNumericValue(Cols.PTIME, -1);
                        stat.addNumericValue(Cols.STIME, -1);

                        StatisticsCSVExporter xp = new StatisticsCSVExporter(stat);
                        Path p = Paths.get(this.queriesOut +  "/" +(new File(queryFile)).getName() + ".stats.csv");
                        xp.write(p);

                        continue; // unmmappable skip!
                    }

                    // The nodes from which we start neighrborhood exploration
                    for (Long conc : queryGraph.vertexSet()) {
                        neighbourStartingNodes.add(conc);
                    }

                    // The edge labels for priortizing exploration
                    for (Edge edge : queryGraph.edgeSet()) {
                        queryEdgeLabels.add(edge.getLabel());
                    }

                    //debug("Time to load query %s: %dms and find it in the graph", queryFile, globalWatch.getElapsedTimeMillis());

                    // Multiple Iterations for the same query
                    for (int experimentIterations = 0; experimentIterations < repetitions; experimentIterations++) {
                        debug("Repetition %d", experimentIterations);
                        if(processError){
                            error("Skipping iteration, pruning has gone wrong");
                            break;
                        }



                        try {
                            StopWatch watch = new StopWatch();

                            Multigraph neighborhood = null;
                            Multigraph restrictedNeighborhood = null;

                            stat.addStringValue(Cols.QUERY, queryFile);
                            Map<Long, Set<Long>> queryGraphMap = null;


                            watch.start();
                            //2. PRUNE THE SPACE
                            //2.1: APPV COMPUTE POPULARITIES
                            watch.reset();
                            if (!skipNeighborhood) {

                                ppv.setStartingNodes(neighbourStartingNodes);
                                ppv.setThreshold(threshold);
                                ppv.setRestartProbability(restartProb);
                                ppv.setK(this.neighborSize);
                                ppv.setLabelFrequencies(informativeness);
                                ppv.setPriorityEdges(queryEdgeLabels);
                                ppv.setKeepOnlyQueryEdges(true);
                                ppv.setHubs(bighubs);
                                ppv.compute();
                                neighborhood = ppv.getNeighborhood();

                                debug("Time to get the most important neighbors: %dms", watch.getElapsedTimeMillis());
                                stat.addNumericValue(Cols.NTIME,watch.getElapsedTimeMillis());

                                debug("Neighbors contains %d edges and %d vertexes", neighborhood.edgeSet().size(), neighborhood.vertexSet().size());
                                stat.addNumericValue(Cols.NSIZE_E, neighborhood.edgeSet().size());
                                stat.addNumericValue(Cols.NSIZE_V, neighborhood.numberOfNodes());


                            } else {
                                debug("Skipping neighborhood!");
                                neighborhood = new BaseMultigraph();

                                debug("Time to get the most important neighbors: %dms", watch.getElapsedTimeMillis());
                                debug("NaiveNeighbors contains %d edges and %d vertexes", neighborhood.edgeSet().size(), neighborhood.vertexSet().size());
                                stat.addNumericValue(Cols.NTIME,-1);
                                stat.addNumericValue(Cols.NSIZE_E, -1);
                                stat.addNumericValue(Cols.NSIZE_V, -1);

                            }

                            //2.3: TABLE PRUNING
                            if (!skipPruning) {
                                //Find related

                                //2.4: COMPUTE TABLES
                                if (graphTables == null || queryTables == null ) {
                                    debug("Computing %d-neighbors tables", k);
                                    watch.reset();
                                    tableAlgorithm.setK(k);
                                    tableAlgorithm.setNumThreads(TABLE_CORES);
                                    tableAlgorithm.setGraph(neighborhood);
                                    tableAlgorithm.compute();
                                    graphTables = tableAlgorithm.getNeighborTables();
                                    tableAlgorithm.setGraph(queryGraph);
                                    tableAlgorithm.compute();
                                    queryTables = tableAlgorithm.getNeighborTables();
                                    debug("Time to compute the graph and query tables: %dms", watch.getElapsedTimeMillis());
                                    stat.addNumericValue(Cols.TTIME,watch.getElapsedTimeMillis());
                                } else {
                                    stat.addNumericValue(Cols.TTIME,0);
                                }


                                //2.5: COMPUTE SIMULATION MAPPING
                                debug("Pruning with %d-neighbors tables", k);
                                watch.reset();
                                pruningAlgorithm.setGraph(neighborhood);
                                pruningAlgorithm.setQuery(queryGraph);
                                pruningAlgorithm.setGraphTables(graphTables);
                                pruningAlgorithm.setQueryTables(queryTables);
                                pruningAlgorithm.compute();
                                queryGraphMap = pruningAlgorithm.getQueryGraphMapping();

                                // CHECK WHETHER PRUNING WAS CORRECT
                                for (Long node : neighbourStartingNodes) {
                                    if (!queryGraphMap.containsKey(node) || queryGraphMap.get(node).isEmpty()) {
                                        error("Query tables do not contain maps for the node " + FreebaseConstants.convertLongToMid(node));
                                        stat.addNumericValue(Cols.PTIME,watch.getElapsedTimeMillis());
                                        stat.addNumericValue(Cols.PSIZE_E,-1);
                                        stat.addNumericValue(Cols.PSIZE_V, -1);
                                        stat.addNumericValue(Cols.STIME, 0);
                                        stat.addNumericValue(Cols.SOLUTIONS, -1);
                                        throw new AlgorithmExecutionException("Error while Pruning: nodes not mapped ");
                                    }
                                }

                                //2.6 PRUNE THE SPACE
                                restrictedNeighborhood = pruningAlgorithm.pruneGraph();
                                debug("Time to compute the graph mapping and prune the graph: %dms, found", watch.getElapsedTimeMillis());
                                stat.addNumericValue(Cols.PTIME,watch.getElapsedTimeMillis());
                                stat.addNumericValue(Cols.PSIZE_E,restrictedNeighborhood.edgeSet().size());
                                stat.addNumericValue(Cols.PSIZE_V, restrictedNeighborhood.numberOfNodes());

                            } else {
                                stat.addNumericValue(Cols.TTIME,-1);
                                stat.addNumericValue(Cols.PTIME,-1);
                                stat.addNumericValue(Cols.PSIZE_V,-1);
                                stat.addNumericValue(Cols.PSIZE_E,-1);
                            }

                            //3: SEARCH RELATED QUERIES
                            watch.reset();
                            HashSet<RelatedQuery> relatedQueriesUnique;
                            List<RelatedQuery> relatedQueries;

                            IsomorphicQuerySearch isoAlgorithm = new IsomorphicQuerySearch();

                            isoAlgorithm.setQuery(queryGraph);
                            isoAlgorithm.setGraph( skipPruning ? neighborhood : restrictedNeighborhood);
                            isoAlgorithm.setNumThreads(this.cores);
                            isoAlgorithm.setQueryToGraphMap(pruningAlgorithm.getQueryGraphMapping());
                            isoAlgorithm.setLimitedComputation(limitComputation);
                            isoAlgorithm.compute();

                            relatedQueries = isoAlgorithm.getRelatedQueries();
                            stat.addNumericValue(Cols.STIME, watch.getElapsedTimeMillis());

                            relatedQueriesUnique = new HashSet<>(relatedQueries);
                            stat.addNumericValue(Cols.SOLUTIONS, relatedQueriesUnique.size());
                            debug("Found %d related queries of which uniques are %d", relatedQueries.size(), relatedQueriesUnique.size());


                            //4: RANK QUERIES
                            if (experimentIterations == 0 && topK > 0) {
                                int a = 0;
                                double max = 0, min = Double.MAX_VALUE, value;
                                double pop = 0, similarity = 0;

                                TreeSet<RelatedQuery> orderedQueriesIntersect = new TreeSet<>();
                                TreeSet<RelatedQuery> orderedQueriesNoIntersect = new TreeSet<>();

                                info("Ordering related queries");

                                watch.reset();
                                info("Recompute Neighborhood and vectors for ranking");
                                ppv.setKeepOnlyQueryEdges(false);
                                ppv.compute();
                                neighborhood = ppv.getNeighborhood();
                                debug("Computing %d-neighbors tables", k);
                                tableAlgorithm.setGraph(neighborhood);
                                tableAlgorithm.compute();
                                NeighborTables tempGraphTables = tableAlgorithm.getNeighborTables();
                                info("Recomputed Neighborhood and vectors for ranking in %dms", watch.getElapsedTimeMillis());
                                //Normalize popularities
                                Map<Long, Double> popularities = new HashMap(ppv.getPopularity());
                                Set<Long> keys = popularities.keySet();

                                for (Long n : keys) {
                                    value = popularities.get(n);
                                    if (value > max) {
                                        max = value;
                                    }
                                    if (value < min) {
                                        min = value;
                                    }
                                }
                                for (Long n : keys) {
                                    popularities.put(n, (popularities.get(n) - min) / (max - min));
                                }

                                for (RelatedQuery relQuery : relatedQueriesUnique) {
                                    int intersections = 1;
                                    pop = 0;
                                    similarity = 0;

                                    Set<Long> mappedNodes = relQuery.getUsedNodes();
                                    for (Long q : queryGraph.vertexSet()) {
                                        if (mappedNodes.contains(q)) {
                                            intersections++;
                                        }
                                    }
                                    intersections *= intersections;
                                    for (Long q : neighbourStartingNodes) {
                                        for( Long n1 : relQuery.mapOf(q)){

                                            if (popularities.containsKey(n1)) {
                                                pop += popularities.get(n1);
                                            }

                                            similarity += score(tempGraphTables.getNodeMap(q), tempGraphTables.getNodeMap(n1));

                                        }

                                        relQuery.addWeight(q, (((lambda * similarity ) + (1 - lambda) * pop)/relQuery.mapOf(q).size()));
                                    }
                                    if(intersections == 1){
                                       orderedQueriesNoIntersect.add(relQuery);
                                    } else {
                                       orderedQueriesIntersect.add(relQuery);
                                    }

                                }

                                info("Ranked related queries in %dms", watch.getElapsedTimeMillis());


                                //9: PRINT RELATED
                                info("Top-%d related queries", topK);


                                Statistics rank = new Statistics();
                                rank.addNumericField("Position");
                                rank.addNumericField("Weight");
                                rank.addStringField("Query");
                                rank.addNumericField("Intersect");

                                for(int type = 0;  type<=1 ;type ++ ){
                                    NavigableSet<RelatedQuery> sortedSet = (type == 0 ? orderedQueriesNoIntersect : orderedQueriesIntersect ).descendingSet();
                                    a=0;
                                    for (RelatedQuery rQuery : sortedSet) {
                                        a++;
                                        if (a <= topK || a == sortedSet.size() ) {
                                            rank.addNumericValue("Position", a);
                                            rank.addNumericValue("Intersect", type);
                                            rank.addNumericValue("Weight", rQuery.getTotalWeight());

                                            String s = String.format("[Q%d,value=%f]", a, rQuery.getTotalWeight());
                                            String q = "";
                                            Set<Edge> mappedEdges = rQuery.getUsedEdges();

                                            int tempEdgeCnt = 0;
                                            for (Edge edge : mappedEdges ) {
                                                if(tempEdgeCnt>0){
                                                    q+= " | ";
                                                }
                                                q += (FreebaseConstants.convertLongToMid(edge.getSource()))
                                                        + "->"
                                                        + ( FreebaseConstants.convertLongToMid(edge.getDestination()));
                                                tempEdgeCnt++;
                                            }
                                            rank.addStringValue("Query", q);
                                            debug(s + q);
                                        }
                                    }
                                }




                                StatisticsCSVExporter rankxp = new StatisticsCSVExporter(rank);
                                Path p = Paths.get(this.queriesOut +  "/" +(new File(queryFile)).getName() + ".top-"+ topK +".csv");
                                rankxp.write(p);

                            } //END IF i==0

                        } catch (AlgorithmExecutionException ex) {
                            processError = true;
                        }

                    } //END FOR REPETITIONS


                    StatisticsCSVExporter xp = new StatisticsCSVExporter(stat);
                    Path p = Paths.get(this.queriesOut +  "/" +(new File(queryFile)).getName() + ".stats.csv");
                    xp.write(p);


                    aggStat.addStringValue(Cols.QUERY, (new File(queryFile)).getName());
                    aggStat.addNumericValue(Cols.NSIZE_E,stat.getAverage(Cols.NSIZE_E) );
                    aggStat.addNumericValue(Cols.NSIZE_V, stat.getAverage(Cols.NSIZE_V));
                    aggStat.addNumericValue(Cols.PSIZE_E, stat.getAverage(Cols.PSIZE_E));
                    aggStat.addNumericValue(Cols.PSIZE_V, stat.getAverage(Cols.PSIZE_V));
                    aggStat.addNumericValue(Cols.SOLUTIONS, stat.getAverage(Cols.SOLUTIONS));
                    aggStat.addNumericValue(Cols.NTIME, stat.getAverage(Cols.NTIME));
                    aggStat.addNumericValue(Cols.TTIME, stat.getAverage(Cols.TTIME));
                    aggStat.addNumericValue(Cols.PTIME, stat.getAverage(Cols.PTIME));
                    aggStat.addNumericValue(Cols.STIME, stat.getAverage(Cols.STIME));



                    fileProgress++;
                    info("STATUS:   Parsed %s out of %s Queries in  %s secs  expected remaing time %s secs", fileProgress, files.size(), globalWatch.getElapsedTimeSecs(), globalWatch.getElapsedTimeSecs()*(files.size()-fileProgress)/(fileProgress*1.0)    );
                    if(fileProgress%20 == 0){
                        //FREE MAIN MEMORY (hopefully)
                        debug("GC");
                        System.gc();
                        debug("GC Done");

                    }

                } //END FOR FILES


                StatisticsCSVExporter aggxp = new StatisticsCSVExporter(aggStat);
                Path p = Paths.get(this.queriesOut +  "/aggregate.stats.csv");
                aggxp.write(p);



            } //END CHECK FILE EMPTY
        } catch (ParseException ex) {
            fatal("Query parsing failed", ex);
        } catch (IOException ioex) {
            fatal("Unable to open query files: %s ", ioex, query);
            throw new ExecutionException(ioex);
        } catch (NullPointerException | IllegalStateException | LoadException ex) {
            fatal("Something wrong happened, message: %s", ex.getMessage());
            ex.printStackTrace();
            throw new ExecutionException(ex);
        }
    }






    /**
     * See Equation 5 in the paper
     * @param vector
     * @param levelNodes
     * @param level
     */
    protected void updateVector(Map<Long, Double> vector, Map<Long, Integer> levelNodes,  int level) {
        Set<Long> labels = levelNodes.keySet();
        Double value;
        int sqLevel = level * level;

        for (Long label : labels) {
            value = vector.get(label);
            if (value == null) {
                value = 0.0;
            }
            value += (levelNodes.get(label) * informativeness.get(label)) / sqLevel;

            vector.put(label, value);
        }
    }

    /**
     * Structural similarity between two nodes
     * @param node1
     * @param node2
     * @return
     */
    protected double score(Map<Long, Integer>[] node1, Map<Long, Integer>[] node2) {
        assert node1.length == node2.length;
        Map<Long, Double> vector1 = new HashMap<>(), vector2 = new HashMap<>();

        for (int i = 0; i < node1.length; i++) {
            updateVector(vector1, node1[i], (i + 1));
            updateVector(vector2, node2[i], (i + 1));
        }
        return VectorSimilarities.cosine(vector1, vector2, true);
    }


    /**
     * Find Graph files and try t load them
     * @param queryPath
     * @return
     */
    protected List<String>  loadQueryFiles( String queryPath ){
            File dir = new File(queryPath);
            List<String> files = new ArrayList<>();
            String tmp;
            if (dir.isDirectory()) {
                debug("Loading graph from directory " + dir.getAbsolutePath());

                File[] childFiles = dir.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File child) {
                        return !child.isDirectory() && child.getName().endsWith(".query");
                    }
                });

                for (File child : childFiles) {
                    try {
                        tmp = child.getCanonicalPath();

                        debug("Adding graph: " + tmp);
                        GraphFilesManager.loadGraph(tmp);
                        files.add(tmp);
                    } catch ( IOException | ParseException | LoadException | NullPointerException e) {
                        error("Query %s is not parsable! ", child.getAbsolutePath());
                        error("Cause is " + e.getMessage());
                        //e.printStackTrace();
                    }
                }
            } else {
                try {
                    tmp = dir.getCanonicalPath();
                    debug("Adding single graph: " + tmp);

                    GraphFilesManager.loadGraph(tmp);
                    files.add(tmp);
                } catch (IOException | ParseException | LoadException | NullPointerException e) {
                    error("Query %s is not parsable! ", queryPath);
                    error("Cause is " + e.getMessage());
                    //Logger.getLogger(TestExemplar.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return files;
    }
    /**
     * Checks whether the query is in the knowledge graph
     * @param queryGraph
     * @return
     */
    protected boolean isQueryMappable(Multigraph queryGraph) {

        for (Long node : queryGraph.vertexSet()) {
            if (queryGraph.outDegreeOf(node) > graph.outDegreeOf(node)) {
                error("Query is not mappable in this Knowledge Graph, missing node %s", FreebaseConstants.convertLongToMid(node) );
                return false;
            }

            Collection<Edge> edges = graph.outgoingEdgesOf(node);

            for (Edge e : queryGraph.outgoingEdgesOf(node)) {
                if (!edges.contains(e)) {
                    error("Query is not mappable in this Knowledge Graph, missing edge %s", FreebaseConstants.convertLongToMid(e.getSource()) + " -> " +  FreebaseConstants.convertLongToMid(e.getDestination()));
                    return false;
                }
            }
        }
        return true;
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
            consoleFormat = "-k",
            defaultValue = "3",
            mandatory = false,
            description = "parameter of the k-neighborhood")
    public void setK(int k) {
        this.k = k;
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
    public void setStartPopularity(int size) {
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
            consoleFormat = "-q",
            defaultValue = "",
            description = "input query",
            mandatory = true)
    public void setQuery(String query) {
        this.query = query;
    }

    @CommandInput(
            consoleFormat = "-kb",
            defaultValue = "",
            description = "path to the knowledgbase sin and sout files, just up to the prefix, like InputData/freebase ",
            mandatory = true)
    public void setKbPath(String kb) {
        this.kbPath = kb;
    }

    @CommandInput(
            consoleFormat = "-l",
            defaultValue = "0.5",
            description = "lambda used in the scoring function",
            mandatory = false)
    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    @CommandInput(
            consoleFormat = "-qout",
            defaultValue = "related_queries",
            description = "the name of the queries output file",
            mandatory = false)
    public void setQueriesOut(String queriesOut) {
        this.queriesOut = queriesOut;
    }

    @DynamicInput(
            consoleFormat = "--graph",
            description = "multigraph used as a knowledge-base")
    public void setGraph(BigMultigraph graph) {
        this.graph = graph;
    }

    @CommandInput(
            consoleFormat = "-lf",
            defaultValue = "",
            mandatory = true,
            description = "label frequency file formatted as 'labelid frequency'")
    public void setLabelFrequencies(String labelFrequencies) {
        this.labelFrequenciesFile = labelFrequencies;
    }

    @CommandInput(
            consoleFormat = "-h",
            defaultValue = "",
            mandatory = true,
            description = "node big hubs file")
    public void setHubs(String hubs) {
        this.hubsFile = hubs;
    }

    @CommandInput(
            consoleFormat = "-r",
            defaultValue = "10",
            mandatory = false,
            description = "number of repetitions per test")
    public void setRepetitions(int repetitions) {
        this.repetitions = repetitions;

    }

    @CommandInput(
            consoleFormat = "--limit",
            defaultValue = "false",
            mandatory = false,
            description = "limit computation of isomorphism when too many results are found")
    public void setLimitComputation(boolean doLimit) {
        this.limitComputation = doLimit;

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
            consoleFormat = "--cores",
            defaultValue = "24",
            mandatory = false,
            description = "core to use for parallelism")
    public void setNumCores(int cores) {
        this.cores = cores;
    }


}




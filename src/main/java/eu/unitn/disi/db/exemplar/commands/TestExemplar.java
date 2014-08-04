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

import eu.unitn.disi.db.command.Command;
import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.DynamicInput;
import eu.unitn.disi.db.command.ParametersNumber;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.VectorSimilarities;
import eu.unitn.disi.db.exemplar.core.algorithms.ComputeGraphNeighbors;
import eu.unitn.disi.db.exemplar.core.algorithms.PersonalizedPageRank;
import eu.unitn.disi.db.exemplar.core.algorithms.PruningAlgorithm;
import eu.unitn.disi.db.exemplar.core.algorithms.RelatedQuery;
import eu.unitn.disi.db.exemplar.core.algorithms.RelatedQueryRecursive;
import eu.unitn.disi.db.exemplar.stats.GraphFilesManager;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TestExemplar extends Command {

    private static final int NUM_CORES = 8;
    private BigMultigraph graph;
    private String query;
    private int neighborSize;
    private int topK;
    private int repetitions;
    private double threshold;
    private double restartProb;
    private double lambda;
    private boolean skipPruning;
    private boolean skipNeighborhood;
    private String labelFrequenciesFile;
    private String kbPath;
    private int k;
    private String queriesOut;
    
    private HashMap<Long, Double> informativeness;
    

    @Override
    protected void execute() throws ExecutionException {
        StopWatch watch;
        List<String> files = new ArrayList<>();
        PersonalizedPageRank ppv;
        Multigraph neighborhood = null;
        Multigraph queryGraph;
        Map<Long, Set<Long>> queryGraphMap;
        List<RelatedQuery> relatedQueries;
        ComputeGraphNeighbors tableAlgorithm;
        PruningAlgorithm pruningAlgorithm;
        NeighborTables graphTables = null, queryTables = null;
        List<Long> nodes;
        int count = 0;
        Map<Long, String> nodeNames;
        StringBuilder sb;

        File queriesOutDir = new File(queriesOut);
        if (!queriesOutDir.exists() || !queriesOutDir.isDirectory()) {
            if (!queriesOutDir.mkdir()) {
                throw new ExecutionException("Cannot create the directory %s to store queries", queriesOut);
            }
        }

        try {
            watch = new StopWatch();

            String tmp;
            File dir = new File(this.query);
            if (dir.isDirectory()) {
                debug("Loading graph from directory " + dir.getAbsolutePath());

                File[] childFiles = dir.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File child) {
                        return !child.isDirectory() && child.getName().endsWith(".query");
                    }
                });

                for (File child : childFiles) {
                    tmp = child.getAbsolutePath();
                    try {
                        debug("Adding graph: " + child.getAbsolutePath());
                        queryGraph = GraphFilesManager.loadGraph(tmp);
                        files.add(tmp);
                    } catch (Exception e) {
                        error("Query %s is not parsable! ", tmp);
                        //e.printStackTrace();
                    }
                }
            } else {
                debug("Adding single graph: " + dir.getAbsolutePath());
                files.add(dir.getCanonicalPath());
            }
            informativeness = new HashMap<>();
            Utilities.readFileIntoMap(labelFrequenciesFile, " ", informativeness, Long.class, Double.class);

            double frequencies = 0.0;
            for (double freq : informativeness.values()) {
                frequencies += freq;
            }

            
            double labelPrior = 0;
            Set<Long> labels = informativeness.keySet();
            for (Long label : labels) {
                labelPrior = -Math.log(informativeness.get(label) / (frequencies));
                informativeness.put(label, labelPrior);
            }

            watch.start();
            if (graph == null) {
                graph = new BigMultigraph(this.kbPath + "-sin.graph", this.kbPath + "-sout.graph"/*, knowledgebaseFileSize*/);
                info("Loaded graph from file-path %s", this.kbPath);
            }
            if (graph == null) {
                throw new IllegalStateException("Null Knowledgebase!!");
            }

            info("Loaded freebase into main-memory in %dms", watch.getElapsedTimeMillis());

            watch.reset();

            if (!files.isEmpty()) {
                for (String queryFile : files) {
                    Long id, mostPopularNode = 0L;
                    long n1;
                    int a = 0;
                    double max = 0, min = Double.MAX_VALUE, value;

                    TreeSet<RelatedQuery> orderedQueries = new TreeSet<>();
                    Map<Long, Double> queryLabelWeights;
                    Map<Long, Long> mappedNodes;
                    Map<Long, Double> popularities;
                    HashSet<RelatedQuery> relatedQueriesUnique;
                    Set<Long> keys;
                    Collection<Long> startingNodes = new HashSet<Long>(); 
                    
                    count++;
                    queryGraphMap = null;

                    watch.reset();
                    debug("Now loading : " + queryFile);
                    queryGraph = GraphFilesManager.loadGraph(queryFile);

                    nodeNames = new HashMap<>();
                    sb = new StringBuilder();
                    //1: LOAD THE GRAPH
                    for (int i = 0; i < repetitions; i++) {
                        queryLabelWeights = new HashMap<>();
                        debug("Repetition %d", i);

                        //2: COMPUTE INITIAL PARTICLES
                        for (Edge edge : queryGraph.edgeSet()) {
                            queryLabelWeights.put(edge.getLabel(), informativeness.get(edge.getLabel()));
                        }
                        //Assign initial particles to nodes in the query
                        double nodeParticles = 0, queryParticles = 0, maxNodeParticles = 0;
                        for (Long conc : queryGraph.vertexSet()) {
                            id = conc;
                            startingNodes.add(id);
                            nodeParticles = (1 / (double) queryGraph.vertexSet().size()) / (this.threshold);
                            queryParticles += nodeParticles;
                            if (nodeParticles > maxNodeParticles) {
                                mostPopularNode = id;
                            }
                            maxNodeParticles = maxNodeParticles < nodeParticles ? nodeParticles : maxNodeParticles;
                        }

                        debug("Time to load query %s: %dms", queryFile, watch.getElapsedTimeMillis());

                        //3: COMPUTE POPULARITIES
                        watch.reset();
                        if (!skipNeighborhood) {
                            ppv = new PersonalizedPageRank(graph);
                            ppv.setStartingNodes(startingNodes);
                            ppv.setThreshold(threshold);
                            ppv.setRestartProbability(restartProb);
                            ppv.setK(this.neighborSize);
                            ppv.setLabelFrequencies(informativeness);
                            ppv.setKeepOnlyQueryEdges(true);
                            neighborhood = ppv.getNeighborhood();

                            debug("Time to get the most important neighbors: %dms", watch.getElapsedTimeMillis());
                            debug("Neighbors contains %d edges and %d vertexes", neighborhood.edgeSet().size(), neighborhood.vertexSet().size());
                            popularities = new HashMap(ppv.getPopularity());
                        } else {
                            debug("Skipping pruning!");
                            neighborhood = new BaseMultigraph();

                            debug("Time to get the most important neighbors: %dms", watch.getElapsedTimeMillis());
                            debug("NaiveNeighbors contains %d edges and %d vertexes", neighborhood.edgeSet().size(), neighborhood.vertexSet().size());
                            popularities = new HashMap();
                        }
                        if (!skipPruning) {
                            //Find related
                            if (i == 0) {
                                //4: COMPUTE TABLES
                                debug("Computing %d-neighbors tables", k);
                                watch.reset();
                                tableAlgorithm = new ComputeGraphNeighbors();
                                tableAlgorithm.setK(k);
                                tableAlgorithm.setNumThreads(NUM_CORES);
                                tableAlgorithm.setGraph(neighborhood);
                                tableAlgorithm.compute();
                                graphTables = tableAlgorithm.getNeighborTables();
                                tableAlgorithm.setGraph(queryGraph);
                                tableAlgorithm.compute();
                                queryTables = tableAlgorithm.getNeighborTables();
                                debug("Time to compute the graph and query tables: %dms", watch.getElapsedTimeMillis());
                            }

                            //5: PRUNE THE SPACE
                            debug("Pruning with %d-neighbors tables", k);
                            watch.reset();
                            pruningAlgorithm = new PruningAlgorithm();
                            pruningAlgorithm.setGraph(neighborhood);
                            pruningAlgorithm.setQuery(queryGraph);
                            pruningAlgorithm.setGraphTables(graphTables);
                            pruningAlgorithm.setQueryTables(queryTables);
                            pruningAlgorithm.compute();
                            queryGraphMap = pruningAlgorithm.getQueryGraphMapping();
                            debug("Time to compute the graph mapping: %dms, found", watch.getElapsedTimeMillis());
                        }

                        //7: COMPUTE RELATED QUERIES
                        watch.reset();
                        RelatedQueryRecursive isoAlgorithm = new RelatedQueryRecursive(queryGraph);
                        relatedQueries = isoAlgorithm.findRelated(queryGraph, neighborhood, queryGraphMap);
                        relatedQueriesUnique = new HashSet<>(relatedQueries);
                        debug("Found %d related queries of which uniques are %d", relatedQueries.size(), relatedQueriesUnique.size());

                        keys = popularities.keySet();
                        //nodes = new ArrayList<Long>();
                        if (i == 0) {
                            //8: RANK QUERIES
                            double pop = 0;
                            info("Ordering related queries");
                            watch.reset();
                            watch.start();
                            nodes = new ArrayList<>();
                            //Normalize popularities
                            Set<Long> consideredNodes = new HashSet<>();
                            for (RelatedQuery relQuery : relatedQueriesUnique) {
                                mappedNodes = relQuery.getMappedConcepts();
                                for (Long node : mappedNodes.values()) {
                                    consideredNodes.add(node);
                                }
                            }
                            info("Computed node vectors in %dms", watch.getElapsedTimeMillis());
                            //Normalize popularities            
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
                                mappedNodes = relQuery.getMappedConcepts();
                                for (Long q : queryGraph.vertexSet()) {
                                    if (mappedNodes.get(q).equals(q)) {
                                        intersections++;
                                    }
                                }
                                intersections *= intersections;
                                for (Long q : queryGraph.vertexSet()) {
                                    n1 = mappedNodes.get(q);
                                    //nodes.add(n1);
                                    if (!popularities.containsKey(n1)) {
                                        pop = 0.0;
                                    } else {
                                        pop = popularities.get(n1);
                                    }
                                    relQuery.addWeight(mappedNodes.get(q), (lambda * score(queryTables.getNodeMap(q), graphTables.getNodeMap(n1)) + (1 - lambda) * pop) / (intersections));
                                }
                                orderedQueries.add(relQuery);
                            }

                            info("Ranked related queries in %dms", watch.getElapsedTimeMillis());
                        } //END IF i==0

                        //9: PRINT RELATED
                        info("Top-%d related queries", topK);
                        for (RelatedQuery rQuery : orderedQueries.descendingSet()) {
                            a++;
                            String s = String.format("[Q%d,value=%f]", a, rQuery.getTotalWeight());
                            for (Edge edge : rQuery.getMappedEdges().values()) {
                                s += (nodeNames.get(edge.getSource()) != null ? nodeNames.get(edge.getSource()) : edge.getSource())
                                        + "->"
                                        + (nodeNames.get(edge.getDestination()) != null ? nodeNames.get(edge.getDestination()) : edge.getDestination()) + " | ";
                            }
                            debug(s);
                            if (a >= topK) {
                                break;
                            }
                        }

                    }
                    watch.reset();
                    //FREE MAIN MEMORY (hopefully)
                    System.gc();
                } //END FOR REPETITIONS
            } //END FOR FILES
        } catch (ParseException ex) {
            fatal("Query parsing failed", ex);
        } catch (IOException ioex) {
            fatal("Unable to open query files: %s ", ioex, query);
            throw new ExecutionException(ioex);
        } catch (Exception ex) {
            fatal("Something wrong happened, message: %s", ex.getMessage());
            ex.printStackTrace();
            throw new ExecutionException(ex);
        }
    }

    //See Equation 5 in the paper
    private void updateVector(Map<Long, Double> vector, Map<Long, Integer> levelNodes,  int level) {
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

    private double score(Map<Long, Integer>[] node1, Map<Long, Integer>[] node2) {
        assert node1.length == node2.length;
        Map<Long, Double> vector1 = new HashMap<>(), vector2 = new HashMap<>();

        for (int i = 0; i < node1.length; i++) {
            updateVector(vector1, node1[i], (i + 1));
            updateVector(vector2, node2[i], (i + 1));
        }
        return VectorSimilarities.cosine(vector1, vector2, true);
    }
    
    
    @Override
    protected String commandDescription() {
        return "Compute related queries using mysql flow";
    }

    @CommandInput(
            consoleFormat = "-p",
            defaultValue = "false",
            mandatory = false,
            description = "avoid using the pruning algorithm",
            parameters = ParametersNumber.ONE)
    public void setUsePruning(boolean usePruning) {
        this.skipPruning = usePruning;
    }

    @CommandInput(
            consoleFormat = "-n",
            defaultValue = "false",
            mandatory = false,
            description = "avoid using the neighborhood algorithm",
            parameters = ParametersNumber.ONE)
    public void setUseNeighborhood(boolean useNeighborhood) {
        this.skipNeighborhood = useNeighborhood;
    }

    @CommandInput(
            consoleFormat = "-topk",
            defaultValue = "10",
            mandatory = false,
            description = "number of related queries to output (top-k)",
            parameters = ParametersNumber.TWO)
    public void setTopK(int topK) {
        this.topK = topK;
    }

    @CommandInput(
            consoleFormat = "-k",
            defaultValue = "3",
            mandatory = false,
            description = "parameter of the k-neighborhood",
            parameters = ParametersNumber.TWO)
    public void setK(int k) {
        this.k = k;
    }

    @CommandInput(
            consoleFormat = "-c",
            defaultValue = "0.15",
            mandatory = false,
            description = "restart probability",
            parameters = ParametersNumber.TWO)
    public void setDispersion(double c) {
        this.restartProb = c;
    }

    @CommandInput(
            consoleFormat = "-s",
            defaultValue = "5000",
            mandatory = false,
            description = "neighbor size",
            parameters = ParametersNumber.TWO)
    public void setStartPopularity(int size) {
        this.neighborSize = size;
    }

    @CommandInput(
            consoleFormat = "-t",
            defaultValue = "1",
            mandatory = false,
            description = "pruning threshold",
            parameters = ParametersNumber.TWO)
    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    @CommandInput(
            parameters = ParametersNumber.TWO,
            consoleFormat = "-q",
            defaultValue = "",
            description = "input query",
            mandatory = true)
    public void setQuery(String query) {
        this.query = query;
    }

    @CommandInput(parameters = ParametersNumber.TWO,
            consoleFormat = "-kb",
            defaultValue = "",
            description = "path to the knowledgbase sin and sout files, just up to the prefix, like InputData/freebase ",
            mandatory = true)
    public void setKbPath(String kb) {
        this.kbPath = kb;
    }

    @CommandInput(parameters = ParametersNumber.TWO,
            consoleFormat = "-l",
            defaultValue = "0.5",
            description = "lambda used in the scoring function",
            mandatory = false)
    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    @CommandInput(parameters = ParametersNumber.TWO,
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
            description = "label frequency file formatted as 'labelid frequency'",
            parameters = ParametersNumber.TWO)
    public void setLabelFrequencies(String labelFrequencies) {
        this.labelFrequenciesFile = labelFrequencies;
    }

    @CommandInput(
            consoleFormat = "-r",
            defaultValue = "10",
            mandatory = false,
            description = "number of repetitions per test",
            parameters = ParametersNumber.TWO)
    public void setRepetitions(int repetitions) {
        this.repetitions = repetitions;

    }

    private static class JsonRelated {

        private int key;
        private Map<String, String> query;
        private Multigraph graph;
        private Map<Long, String> names;
        private Collection<RelatedQuery> top_embedded;
        private Collection<RelatedQuery> top_external;
        private Collection<RelatedQuery> bottom;
        private Collection<RelatedQuery> random;
        private Collection<String> google;
        private Collection<String> bing;

        public JsonRelated() {
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public Map<String, String> getQuery() {
            return query;
        }

        public void setQuery(Map<String, String> query) {
            this.query = query;
        }

        public Multigraph getGraph() {
            return graph;
        }

        public void setGraph(Multigraph graph) {
            this.graph = graph;
        }

        public Collection<String> getGoogle() {
            return google;
        }

        public void setGoogle(Collection<String> google) {
            this.google = google;
        }

        public Collection<String> getBing() {
            return bing;
        }

        public void setBing(Collection<String> bing) {
            this.bing = bing;
        }

        public Map<Long, String> getNames() {
            return names;
        }

        public void setNames(Map<Long, String> names) {
            this.names = names;
        }

        public Collection<RelatedQuery> getTop_embedded() {
            return top_embedded;
        }

        public void setTop_embedded(Collection<RelatedQuery> top_embedded) {
            this.top_embedded = top_embedded;
        }

        public Collection<RelatedQuery> getTop_external() {
            return top_external;
        }

        public void setTop_external(Collection<RelatedQuery> top_external) {
            this.top_external = top_external;
        }

        public Collection<RelatedQuery> getBottom() {
            return bottom;
        }

        public void setBottom(Collection<RelatedQuery> bottom) {
            this.bottom = bottom;
        }

        public Collection<RelatedQuery> getRandom() {
            return random;
        }

        public void setRandom(Collection<RelatedQuery> random) {
            this.random = random;
        }
    }
}

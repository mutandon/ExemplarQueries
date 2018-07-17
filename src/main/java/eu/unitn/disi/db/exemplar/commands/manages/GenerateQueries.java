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
package eu.unitn.disi.db.exemplar.commands.manages;


import com.koloboke.collect.map.hash.HashLongIntMaps;
import com.koloboke.collect.set.hash.HashLongSets;
import eu.unitn.disi.db.exemplar.commands.tests.exq.TestExemplar;
import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.command.util.stats.Statistics;
import eu.unitn.disi.db.command.util.stats.StatisticsCSVExporter;
import eu.unitn.disi.db.exemplar.core.storage.StorableTable;
import eu.unitn.disi.db.exemplar.utils.names.FreebaseNames;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class GenerateQueries extends TestExemplar {

    
    protected int MAX_DEG = 11;
    
    
    
    //protected Map<Long, Integer> labelsOrder;

            
    private String starting;    
    private HashSet<Long> skippable;
    private Random rand;
    private int numFragments;
    private double maxlabelInformativeness = 0.0;
    
    
    
    
    
    /**
     *
     */
    public abstract static class AddedCols {

        /**
         * "Do collapse change the query"
         */
        public static final String ISA = "isA";

    }

    @Override
    protected void execute() throws ExecutionException {
        StopWatch globalWatch;

        File queriesOutDir = this.outDir;
        if (!queriesOutDir.exists() || !queriesOutDir.isDirectory()) {
            if (!queriesOutDir.mkdir()) {
                throw new ExecutionException("Cannot create the directory %s to store queries", queriesOutDir);
            }
        }

        try {
            this.names = new FreebaseNames();
            loadData();

            globalWatch = new StopWatch();
            Statistics stat;
            Long seed =  (System.currentTimeMillis() / 1000);
            this.rand = new Random(seed);            
            info("SEED IS : " + seed);
            
            
                       
            // Prepare Single query Stats
            stat = new Statistics();
            stat.addStringField(Cols.QUERY);
            stat.addNumericField(Cols.NSIZE_E);
            stat.addNumericField(Cols.NSIZE_V);
            stat.addNumericField(AddedCols.ISA);

            debug("Read all vertex");                        
            globalWatch.start();
            graph.numberOfNodes();
            // Iterate through files
            info("Starting generating %s queries with %s fragments  ", this.repetitions, this.numFragments );                        
            for (int queryProgress = 0; queryProgress < this.repetitions; queryProgress++) {
                debug("Current bulding query %d", queryProgress+1);

                
                //DATA
                Multigraph queryGraph = new BaseMultigraph();
                
                HashSet<Long> visitedNodes = new HashSet<>();                
                Set<Long> queryEdgeLabels = new HashSet<>();                                
                Set<Long> restartSet;
                
                final Long startNode = getStartNode();                                                                
                
                
                Long nextNode = startNode;
                Map<Long, Integer> lblFreq =new HashMap<>();
                for(int numF =0; numF< this.numFragments; numF++){                                        
                    int numEdges =0;                    
                    Long prevNode = 0l;
                    debug("Starting node %s has %s edges", nextNode, this.graph.degreeOf(nextNode));
                    queryGraph.addVertex(nextNode);
                    restartSet = new HashSet<>();
                    restartSet.add(nextNode);
                    
                    int limit=10000;
                    Edge toAdd =null;
                    int fragmentSize = this.rand.nextInt(this.neighborSize)+1;
                    while(numEdges < fragmentSize && limit>0){
                        limit--;
                        Iterator<Edge> edgeIter;
                        
                        Long candNode;
                        
                        int degree = this.graph.degreeOf(nextNode);
                        
                        boolean added = false;                        
                        double selectivity =5;
                        double degreeDiv = 2; // should point to desired degree + 1
                        int sameNode = 0;
                        Long candLabel;
                        for(int inOut=0; inOut<2; inOut++){
                            edgeIter = inOut ==0 ? this.graph.outgoingEdgesIteratorOf(nextNode) : this.graph.incomingEdgesIteratorOf(nextNode);
                            while(edgeIter.hasNext()){
                                toAdd = edgeIter.next();
                                candLabel= toAdd.getLabel();
                                candNode = inOut ==0 ? toAdd.getDestination() : toAdd.getSource();
                                boolean inOtherFragments = visitedNodes.contains(candNode);
                                boolean backTurn = candNode.equals(prevNode) || queryGraph.containsEdge(nextNode, candNode)|| queryGraph.containsEdge(candNode,nextNode);
                                boolean sameLabel = lblFreq.getOrDefault(candLabel,0) > (isSkippable(candLabel)?0 : 2);
                                boolean check = null != names.getLabelIDFromName(names.getLabelNameFromID(candLabel));
                                boolean isValidLabel = check && this.labelFrequencies.get(candLabel)>50;
                                
                                
                                if(!inOtherFragments && !backTurn && !sameLabel  && isValidLabel){
                                    //Random step, favors informative labels
                                    double bonus = this.labelInformativeness.get(toAdd.getLabel())*10/maxlabelInformativeness;
                                    
                                    double choice = rand.nextInt(50+50*(int)Math.log1p(degree));                                    
                                    if(isSkippable(toAdd.getLabel())) {
                                        choice*=2;
                                        bonus =0.0;
                                    }                                    
                                    
                                    choice += choice*(queryGraph.degreeOf(nextNode)+sameNode)/degreeDiv; // penalize stars
                                    if(choice -bonus < 1){
                                        //debug("Bonus is %s too big", bonus);
                                        choice = 0.1;
                                    } else {
                                        choice -= bonus;
                                    }
                                    
                                                                        
                                    // For nodes with low degree we need to make sure to follow an edge
                                    if( ( !added && degree<4 && inOut>0) || (choice >0 && choice <selectivity)){
                                        smartAddEdge(queryGraph, toAdd, false);
                                        int f = lblFreq.getOrDefault(candLabel, 0) +1;
                                        lblFreq.put(candLabel,f);
                                        restartSet.add(candNode);
                                        prevNode = nextNode;
                                        nextNode = candNode;                                        
                                        sameNode++;
                                        added = true;
                                        numEdges++;
                                        if(sameNode >1){
                                            selectivity/=2;                                            
                                            break;
                                        }
                                        
                                    }
                                }
                            }
                        }
                        
                        if(!added){ 
                            nextNode = randomObject(restartSet);
                            //debug("Current fragment contains %s nodes %s edges", queryGraph.numberOfNodes(), queryGraph.numberOfEdges()  );
                            // if we failed to select an edge, we restart                            
                            //debug("Added fail for %s, back to %s", nextNode, startNode);                                                
                        }                          
                    }
                    
                    Set<Edge> edgesToAdd = new HashSet<>();
                    if(limit<0 && toAdd != null){
                        edgesToAdd.add(toAdd);
                    }
                    // Close if possible                                        
                    for(Long n1 : restartSet){
                        for(Long n2 : restartSet){
                            if(n1.equals(n2)){
                                continue;
                            }
                            if(!queryGraph.containsEdge(n1, n2) && !queryGraph.containsEdge(n2, n1)){
                                
                                if(this.graph.containsEdge(n1, n2)){
                                    edgesToAdd.addAll(this.graph.getEdge(n1, n2));
                                } else if(this.graph.containsEdge(n2, n1)){
                                    edgesToAdd.addAll(this.graph.getEdge(n2, n1));
                                }
                            }
                        }
                    }
                    
                    if(!edgesToAdd.isEmpty()){
                        debug("Found closure of size %s", edgesToAdd.size());
                        for(Edge e: edgesToAdd){
                            if(!isSkippable(e.getLabel())){
                                smartAddEdge(queryGraph, e, false);                                
                            }                            
                        }
                    }
                    
                    
                    debug("Candidate %s added %s Edges + with %s edges  and %s nodes",numF+1, numEdges, queryGraph.numberOfEdges(), queryGraph.numberOfNodes());
                    // Fragments need to be disconnected
                    visitedNodes.addAll(queryGraph.vertexSet());
                    // Next fragment starts from a node similar to the last added
                    int cluster;
                    boolean foundNext = false;
                    for(Long n : queryGraph){
                        if(this.graph.degreeOf(n)<4){
                            continue;
                        }
                        cluster = this.nodesHahMap.get(n);
                        Set<Long> similarNodes = this.hashInvertedIndex.get(cluster);
                        if(similarNodes.size()> 50){
                            for (int i = 0; i < similarNodes.size()*2; i++) {                        
                                Long l = randomObject(similarNodes);
                                if(!visitedNodes.contains(l)){
                                    nextNode = l;
                                    foundNext=true;                            
                                    //debug("Found  next node %s", l );
                                    break;
                                }
                            }
                        }
                        if(foundNext){
                            break;
                        }
                    }
                                                                                                                                                 
                    if(!foundNext){
                        error("Failed building new fragment");
                        break;
                    }
                                                            
                }
                
                int maxDeg = 0;
                for(Long n : queryGraph){
                    maxDeg = Math.max(maxDeg, queryGraph.degreeOf(n));
                }
                debug("Final Max degree %s", maxDeg);

                List<Multigraph> comps = splitQueries(queryGraph);
                if(comps.size() !=  this.numFragments){
                    error(" +++++++++++++ Generated query with wrong number of componenets %s instead of %s", comps.size(), this.numFragments);
                } else {
                                
                    String q = "";
                    
                    for(Multigraph m : comps){
                        Collection<Edge> mappedEdges = m.edgeSet();                
                        int tempEdgeCnt = 0;
                        for (Edge edge : mappedEdges) {
                            queryEdgeLabels.add(edge.getLabel());
                            if (tempEdgeCnt > 0) {
                                q += "\n";
                            }
                            q += (names.getNodeNameFromID(edge.getSource())) + "\t"
                                    + (names.getNodeNameFromID(edge.getDestination()) + "\t"
                                    + (names.getLabelNameFromID(edge.getLabel())));

                            tempEdgeCnt++;
                        }
                        q += "\n#\n";
                    }                    
                    //debug(q);

                    String queryFileName = this.outDir + "/generated-f"+this.numFragments + "-q" + (queryProgress+1);
                    queryFileName += "-e"+ queryGraph.numberOfEdges()+ "-n" + queryGraph.numberOfNodes()+ "-" + System.currentTimeMillis() + ".query";

                    try (PrintWriter writer = new PrintWriter( queryFileName, "UTF-8")) {                    
                        writer.println(q);                    
                    }

                    debug("Graph Contains %s fragments,  %s Edges and %s nodes", comps.size(), queryGraph.numberOfEdges() , queryGraph.numberOfNodes());
                    debug("Elapsed: " + globalWatch.getElapsedTimeSecs() + " Secs");
                    stat.addStringValue(Cols.QUERY, q);
                    stat.addNumericValue(Cols.NSIZE_V, queryGraph.numberOfNodes());
                    stat.addNumericValue(Cols.NSIZE_E, queryGraph.numberOfEdges());
                    stat.addNumericValue(AddedCols.ISA, queryEdgeLabels.contains(FreebaseNames.ISA_ID) ? 1 : 0);
                }
            }

            StatisticsCSVExporter rankxp = new StatisticsCSVExporter(stat, this.outDir + "/generated-" + this.repetitions + "-" + System.currentTimeMillis() + ".csv");
            rankxp.write();

        } catch (ParseException ex) {
            fatal("Query parsing failed", ex);
        } catch (IOException ioex) {
            fatal("Unable to open query files: ", ioex);
            throw new ExecutionException(ioex);
        } catch (NullPointerException | IllegalStateException ex) {
            fatal("Something wrong happened, message: %s", ex.getMessage());
            ex.printStackTrace();
            throw new ExecutionException(ex);
        }
    }

    
    /**
     * 
     * @param <T>
     * @param coll
     * @return 
     */
    public <T> T randomObject(Collection<T> coll) {
        
        int num = this.rand.nextInt(coll.size());
        for (T t : coll) {
            num--;
            if (num < 0) {
                return t;
            }
        }
        throw new AssertionError();
    }
    

    private long getStartNode(){
        long randomNode;
        if (!this.starting.isEmpty()) {
           return this.names.getNodeIDFromName(this.starting);
        }
        
        //Select random       
        randomNode = rand.nextInt(this.graph.numberOfNodes());
         
        for (Long nodeId : graph) {
            if (randomNode <= 0) {
                randomNode = nodeId;                    
                break;
            }
            //TODO: There is a better way: ask Davide
            randomNode--;
        }
        
        return randomNode;        
    }
    
    
    
    
    
    /**
     * Adds an edge to a multigraph adding the nodes first when needed;
     *
     * @param graph the graph to add to
     * @param edge the edd to add
     * @param stopIfMissing intterrupts
     * @return the new graph with added edge
     */
    public static Multigraph smartAddEdge(Multigraph graph, Edge edge, boolean stopIfMissing) {

        if (!graph.containsVertex(edge.getSource())) {
            if (stopIfMissing) {
                throw new IllegalStateException("Missing source node for edge " + edge);
            }
            graph.addVertex(edge.getSource());
        }
        if (!graph.containsVertex(edge.getDestination())) {
            if (stopIfMissing) {
                throw new IllegalStateException("Missing destination node for edge " + edge);
            }
            graph.addVertex(edge.getDestination());
        }
        //That a good mapping edge, add to the related query
        graph.addEdge(edge.getSource(), edge.getDestination(), edge.getLabel());
        return graph;
    }

    public boolean isSkippable(Long label) throws IOException {
        if (names.getLabelNameFromID(label) == null) {
            return true;
        }

        return skippable.contains(label);
    }

    @SuppressWarnings("unchecked")
    private void loadData() throws IOException, NullPointerException, InvalidClassException, ParseException {
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
        //labelsOrder = new HashMap<>(labelFrequencies.size()*4/3);
        //int idx =0;
        
        for (Long label : labels) {
            labelPrior = (1.0 - labelFrequencies.get(label)*1.0 / (frequencies));
            maxlabelInformativeness = Math.max(maxlabelInformativeness, labelPrior);
            labelInformativeness.put(label, labelPrior);
            //labelsOrder.put(label, idx);
            //idx++;
        }
        debug("Max Label Informativeness is %s", maxlabelInformativeness);                

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

        

                                              
        Runtime runtime = Runtime.getRuntime();                                        
        debug("Memory used is: %.2fMb, free memory: %.2fMb.", (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.), runtime.freeMemory()/(1024*1024.));


        skippable = new HashSet<>();
        skippable.add(names.getLabelIDFromName("/type/object/type"));
        skippable.add(names.getLabelIDFromName("/type/object/permission"));
        skippable.add(names.getLabelIDFromName("/location/location/contains"));
        
        skippable.add(names.getLabelIDFromName("/projects/project_focus/projects"));
        
        skippable.add(names.getLabelIDFromName("/book/book_edition/place_of_publication"));
        skippable.add(names.getLabelIDFromName("/book/author/book_editions_published"));
        
        skippable.add(names.getLabelIDFromName("/book/book_edition/book"));
        skippable.add(names.getLabelIDFromName("/book/book/editions"));
        skippable.add(names.getLabelIDFromName("/book/book_edition/isbn"));
        skippable.add(names.getLabelIDFromName("/book/isbn/book_editions"));

        skippable.add(names.getLabelIDFromName("/music/track/releases"));
        skippable.add(names.getLabelIDFromName("/music/release/track"));
        skippable.add(names.getLabelIDFromName("/music/release/track_list"));
        skippable.add(names.getLabelIDFromName("/music/release_track/recording"));
        skippable.add(names.getLabelIDFromName("/music/track/artist"));        
        skippable.add(names.getLabelIDFromName("/music/artist/track"));
        skippable.add(names.getLabelIDFromName("/music/release/format"));        

        skippable.add(names.getLabelIDFromName("/measurement_unit/dated_integer/source"));
        skippable.add(names.getLabelIDFromName("/measurement_unit/dated_money_value/source"));
        
        
        skippable.add(names.getLabelIDFromName("/film/film/release_date_s"));
        
        skippable.add(names.getLabelIDFromName("/people/person/gender"));

        skippable.add(FreebaseNames.HAS_DOMAIN_ID);
        //skippable.add(FreebaseConstants.ISA_ID);
    }
    
        /**
     * 
     * @param g the query graph
     * @return connected components in the query
     */
    protected List<Multigraph> splitQueries(Multigraph g) {
        final int INIT_SIZE = g.numberOfNodes();
        List<Multigraph> queries = new ArrayList<>(INIT_SIZE);

        Set<Long> visited = new HashSet<>(INIT_SIZE);

        BaseMultigraph tmp;
        Collection<Edge> toAdd;
        Collection<Edge> nextAdd;
        for (long node : g) {
            if(g.degreeOf(node)<1){
                error("Isolated node");
                continue;
            }
            if (!visited.contains(node)) {
                // Build new with BFS
                tmp = new BaseMultigraph(INIT_SIZE);
                toAdd = g.edgesOf(node);
                visited.add(node);
                while (!toAdd.isEmpty()) {
                    nextAdd = new HashSet<>(INIT_SIZE);
                    for (Edge e : toAdd) {
                        tmp.forceAddEdge(e);
                        //Expand
                        for (int it = 0; it < 2; it++) {
                            long tNode = it == 0 ? e.getSource() : e.getDestination();
                            if (!visited.contains(tNode)) {
                                visited.add(tNode);
                                nextAdd.addAll(g.edgesOf(tNode));
                            }
                        }

                    }
                    toAdd = nextAdd;
                }
                queries.add(tmp);
            }
        }

        return queries;
    }


    @Override
    protected String commandDescription() {
        return "Generate queries based on pseudo random walks";
    }

    @CommandInput(
            consoleFormat = "--num",
            defaultValue = "1",
            mandatory = false,
            description = "Number of query fragments")
    public void setSkipHubs(int num) {
        this.numFragments = num;
    }


    @CommandInput(
            consoleFormat = "--start",
            defaultValue = "",
            mandatory = false,
            description = "Set Starting node")
    public void setEarlyStop(String stop) {
        this.starting = stop;
    }

    
}

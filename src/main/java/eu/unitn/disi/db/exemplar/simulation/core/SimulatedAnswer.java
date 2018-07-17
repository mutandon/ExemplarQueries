/*
 * Copyright (C) 2012 Matteo Lissandrini <ml at disi.unitn.eu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package eu.unitn.disi.db.exemplar.simulation.core;



import eu.unitn.disi.db.exemplar.core.ExemplarAnswer;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;

import java.util.ArrayList;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class extends the RelatedQuery class with the Simulated Query thus the
 * mapping between query nodes and graph nodes is just a Simulation
 * (unidirectional)
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */
public class SimulatedAnswer extends ExemplarAnswer{

    /**
     * Query node to Graph node map
     */
    protected Map<Long, List<Long>> mappedNodes;
    //protected Map<Long, List<Long>> reversedMappedNodes;

    /**
     * Query edge to Graph edge map
     */
    protected Map<Edge, List<Edge>> mappedEdges;
    
    protected Set<Edge> usedEdges; 
   

    
    /**
     * Constructor for the class
     *
     * @param query the query that needs to be mapped
     */
    public SimulatedAnswer(Multigraph query) {
        super(query);
        this.initialize();
    }

    /**
     * Initialize all the variables
     */
    private void initialize() {
        final int edgeSize = query.numberOfEdges();
        this.mappedNodes = new HashMap<>(query.vertexSet().size() + 2, 1);
        for (Long c : query.vertexSet()) {
            this.mappedNodes.put(c, new ArrayList<>(edgeSize*4/3));
        }

        //this.reversedMappedNodes = new HashMap<>(edgeSize*2 + 2, 1);
        this.mappedEdges = new HashMap<>(edgeSize + 2, 1);
        for (Edge l : query.edgeSet()) {
            this.mappedEdges.put(l, new ArrayList<>(edgeSize*4/3));
        }

        this.usedEdges = new HashSet<>(edgeSize + 2, 1);
        

    }

    @Override
    public SimulatedAnswer getClone() {
        SimulatedAnswer clone = new SimulatedAnswer(this.query);
        //clone.mappedNodes = new HashMap<>(query.vertexSet().size() + 2, 1f);

        for (Long key : this.mappedNodes.keySet()) {
            //clone.mappedNodes.put(key, new LinkedList<Long>());
            clone.mappedNodes.get(key).addAll(this.mappedNodes.get(key));

        }

        //clone.reversedMappedNodes = new HashMap<>(this.reversedMappedNodes.size() + 2, 1f);
//        for (Long key : this.reversedMappedNodes.keySet()) {
//            clone.reversedMappedNodes.put(key, new LinkedList<Long>());
//            clone.reversedMappedNodes.get(key).addAll(this.reversedMappedNodes.get(key));
//        }
        //clone.usedEdgesIDs = new HashSet<>(this.usedEdgesIDs.size() + 2, 1f);
       // clone.usedEdges.addAll(this.usedEdges);

        clone.usedEdges = new HashSet<>(this.usedEdges.size() + 2, 1f);
        clone.usedEdges.addAll(this.usedEdges);
        
         //clone.mappedEdges = new HashMap<>(query.edgeSet().size() + 2, 1f);
        for (Edge key : this.mappedEdges.keySet()) {
            //clone.mappedEdges.put(key, new LinkedList<Edge>());
            clone.mappedEdges.get(key).addAll(this.mappedEdges.get(key));

        }

        clone.totalWeight = this.totalWeight;
        clone.nodeWeights = new HashMap<>();
        clone.nodeWeights.putAll(this.nodeWeights);

        return clone;
    }

    /**
     * Maps the passed query node to the given node in the graph
     *
     * @param queryNode
     * @param graphNode
     */
    @Override
    public void map(Long queryNode, Long graphNode) {
        if (queryNode == null) {
            throw new IllegalArgumentException("Query Node cannot be null");
        }

        if (graphNode == null) {
            throw new IllegalArgumentException("Graph Node cannot be null");
        }

        if (!this.mappedNodes.containsKey(queryNode)) {
            throw new IllegalArgumentException("Query node " + queryNode + " is not present in the original query");
        }

        mappedNodes.get(queryNode).add(graphNode);

//        if (!reversedMappedNodes.containsKey(graphNode)) {
//            reversedMappedNodes.put(graphNode, new LinkedList<Long>());
//        }
//
//        reversedMappedNodes.get(graphNode).add(queryNode);
    }

    /**
     * Maps the passed query edge to the given edge in the graph
     *
     * @param queryEdge
     * @param graphEdge
     */
    @Override
    public void map(Edge queryEdge, Edge graphEdge) {
        if (queryEdge == null) {
            throw new IllegalArgumentException("Query Edge cannot be null");
        }

        if (graphEdge == null) {
            throw new IllegalArgumentException("Graph Edge cannot be null");
        }

        if (!this.mappedEdges.containsKey(queryEdge)) {
            throw new IllegalArgumentException("Query node " + queryEdge + " is not present in the original query");
        }

        mappedEdges.get(queryEdge).add(graphEdge);
        
        usedEdges.add(graphEdge);
        

    }

    /**
     *
     * @return the list of graph nodes mapped to something
     */
    @Override
    public Set<Long> getUsedNodes() {
        HashSet<Long> out = new HashSet<>();
        for (List<Long> vals : this.mappedNodes.values()) {
            out.addAll(vals);

        }
        return out;
    }

    /**
     *
     * @return a copy of the map from query nodes to graph node
     */
    public Map<Long, List<Long>> getNodesMapping() {
        Map<Long, List<Long>> m = new HashMap<>();

        for (Long key : this.mappedNodes.keySet()) {
            m.put(key, new ArrayList<>());
            m.get(key).addAll(this.mappedNodes.get(key));

        }
        return m;
    }

    /**
     * check whether the queryNode has already been mapped to a Graph Node
     *
     * @param queryNode
     * @return true if the queryNode has been mapped to a graph node
     */
    @Override
    public boolean hasMapped(Long queryNode) {
        return this.mappedNodes.containsKey(queryNode) && this.mappedNodes.get(queryNode) != null && !this.mappedNodes.get(queryNode).isEmpty();
    }

    /**
     * check whether the GraphNode has been mapped to a queryNode
     *
     * @param graphNode
     * @return true if the graphNode has been mapped to a queryNode
     */
    @Override
    public boolean isUsing(Long graphNode) {
        for (List<Long> vals : this.mappedNodes.values()) {
            if (vals.contains(graphNode)) {
                return true;
            }
        }
        // this.reversedMappedNodes.containsKey(graphNode) && this.reversedMappedNodes.get(graphNode) != null && !this.reversedMappedNodes.get(graphNode).isEmpty()
        return false;
    }

    /**
     * check whether the queryEdge has been mapped to a graph Edge
     *
     * @param queryEdge
     * @return true if the queryEdge has been mapped to a graph Edge
     */
    @Override
    public boolean hasMapped(Edge queryEdge) {
        return this.mappedEdges.containsKey(queryEdge) && this.mappedEdges.get(queryEdge) != null && !this.mappedEdges.get(queryEdge).isEmpty();
    }

    /**
     * check whether the graphEdge has been mapped to a queryEdge
     *
     * @param graphEdge
     * @return true if the graphEdge has already been mapped to a queryEdge
     */
    @Override
    public boolean isUsing(Edge graphEdge) {
                
        return this.usedEdges.contains(graphEdge);
        
    }

    /**
     *
     * @return a copy of the list of graph edges ids in use
     */
    @Override
    public Set<String> getUsedEdgesIDs() {
        Set<String> m = new HashSet<>();
        
        
        m.addAll(this.usedEdges.stream().map(e -> e.getId()).collect(Collectors.toSet()));        
        
                
        return m;
    }

    
    
    
    /**
     *
     * @return a copy of the list of graph edges in use
     */
    @Override
    public Set<Edge> getUsedEdges() {        
        return new HashSet<>(this.usedEdges);
    }

    /**
     *
     * @param graphNode
     * @return the a copy of the list of nodes mapping to this graph node
     */
    public List<Long> mappedAs(Long graphNode) {
        List<Long> m = new ArrayList<>();
        //m.addAll(this.reversedMappedNodes.get(graphNode));
        for (Long qNode : this.mappedNodes.keySet()) {
            if (this.mappedNodes.get(qNode).contains(graphNode)) {
                m.add(qNode);
            }
        }

        return m;
    }

    @Override
    public List<Long> mapOf(Long queryNode) {
        List<Long> map = new ArrayList<>();
        map.addAll(this.mappedNodes.get(queryNode));
        return map;
    }

    @Override
    public List<Edge> mapOf(Edge queryEdge) {
        List<Edge> map = new ArrayList<>();
        map.addAll(this.mappedEdges.get(queryEdge));
        return map;
    }

    /**
     * Checks if the simulation mapping is complet, not correctness
     *
     * @return true if all nodes & edges have their mappings
     */
    public boolean isMappingComplete() {
        for (Long key : this.mappedNodes.keySet()) {
            if (this.mappedNodes.get(key) == null || this.mappedNodes.get(key).isEmpty()) {
                //System.err.println("Incomplete for NODE "+ key);                
                return false;
            }
        }

        for (Edge key : this.mappedEdges.keySet()) {
            if (this.mappedEdges.get(key) == null || this.mappedEdges.get(key).isEmpty()) {
                //System.err.println("Incomplete for EDGE "+ key);
                return false;
            }
        }
        return true;
    }

    /**
     * Try to build a graph from the mapped concepts and edges
     *
     * @return
     */
    @Override
    public Multigraph buildMatchedGraph() {
        BaseMultigraph rGraph = new BaseMultigraph();
        
        /*for (Long key : this.mappedNodes.keySet()) {
            if (this.mappedNodes.get(key) == null || this.mappedNodes.get(key).isEmpty()) {
                throw new IllegalStateException("The query is not totally mapped: missing a node ");
            }
            for (Long node : this.mappedNodes.get(key)) {
                queryGraph.addVertex(node);
            }

        }*/

        for (Edge key : this.mappedEdges.keySet()) {
            if (this.mappedEdges.get(key) == null || this.mappedEdges.get(key).isEmpty()) {
                throw new IllegalStateException("The query is not totally mapped: missing an edge");
            }
            for (Edge edge : this.mappedEdges.get(key)) {
                rGraph.forceAddEdge(edge);                
            }
        }
        return rGraph;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        if (obj instanceof SimulatedAnswer) {
            final SimulatedAnswer other = (SimulatedAnswer) obj;

            Set<Edge> otherEdges = other.usedEdges;
            Set<Edge> thisEdges = this.usedEdges;

            if (otherEdges.size() == thisEdges.size()) {
                for (Edge otherEdge : otherEdges) {
                    if (!thisEdges.contains(otherEdge)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        String s = "";
        for (Edge e : this.usedEdges) {
            s += e.getId() + " ";
        }
        return s;
    }

    @Override
    public int hashCode() {
        return this.usedEdges.hashCode();
    }
    
    @Override
    public int size(){
        return this.usedEdges.size();
    }

    /**
     * Computes a collapsed version of the input graph to be used as "simpler"
     * query
     *
     * @param query to be collapsed
     * @return the collapsed version of the input graph (query)
     */
    public static Multigraph collapsedGraph(Multigraph query) {
        //Data Structures
        //HashMap<Long, Long> queryNodeMap = new HashMap<>(); // maps query node to new IDs
        List<List<Long>> blocks = new ArrayList<>(); // this is PI - the set of partitions
        List<List<Long>> newBlocks; // this is PI for the next iteration
        ArrayList<Long> labels = new ArrayList<>(); // The labels of the query
        List<Long> queryNodes = new ArrayList<>(); // the nodes of the query
        HashMap<Long, Integer> inBlockMap = new HashMap<>(); // In which block is each node?
        HashMap<Long, Integer> newInBlockMap; // In which block is each node? for the new step

        // The simulation graph: will contain two unconneted graphs-890
        // The query (with negative indexes) and the graph that shouls simulate it
        Multigraph toCollapse = new BaseMultigraph();
        boolean changed = true;

        //Get the labes of the query
        for (Edge e : query.edgeSet()) {
            labels.add(e.getLabel());
        }

        //Start the big block
        List<Long> pr = new ArrayList<>(2 * (query.vertexSet().size() + 1));

        for (Long node : query.vertexSet()) {
            pr.add(node);
            inBlockMap.put(node, 0);
            queryNodes.add(node);
            toCollapse.addVertex(node);

            if (query.outDegreeOf(node) == 0) {
                for (Edge e : query.incomingEdgesOf(node)) {

                    Long fake = -1 * node;
                    if (!toCollapse.containsVertex(fake)) {
                        pr.add(fake);
                        inBlockMap.put(fake, 0);
                        queryNodes.add(fake);
                        toCollapse.addVertex(fake);
                    }
                    toCollapse.addEdge(node, fake, -1 * e.getLabel());
                    labels.add(-1 * e.getLabel());
                }
            }
        }

        for (Edge e : query.edgeSet()) {
            toCollapse.addEdge(e);
            //toCollapse.addEdge(-1*e.getSource(),-1*e.getDestination(), e.getLabel());
        }

        //The initial partiion contain only the initilal block
        blocks.add(pr);
        // The loop - cotinue since no more splits are done

        while (changed) {
            changed = false;
            newBlocks = new ArrayList<>();
            newInBlockMap = new HashMap<>();
            // Try to split each block
            for (List<Long> block : blocks) {
                // Cicle each label
                boolean replacePrevious = false;

                for (Long label : labels) {
                    // Extract the a-labelled transition FROM states in block
                    //Try to split the block
                    //System.out.println("Splitting with label " + label + " block " + block);
                    List<Long> splitters = CollectionUtilities.intersect(block, queryNodes);

                    List<List<Long>> tempBlocks = split(block, label, splitters, inBlockMap, toCollapse, true);

                    //Replace previous means that we already added this very block
                    if (replacePrevious) {
                        newBlocks.remove(newBlocks.size() - 1);
                    }

                    //If we splitted we need to change the mapping of blocks...
                    //For each block then (they are at most 2)
                    for (List<Long> b : tempBlocks) {
                        // We re-Map each node
                        // Update reachability
                        for (Long node : b) {
                            //The current node is in the block with id the current size (before adding)
                            newInBlockMap.put(node, newBlocks.size());
                        }
                        //Now we can add this new block
                        newBlocks.add(b);
                    }

                    if (tempBlocks.size() > 1) {
                        changed = true;
                        break;
                    } else {
                        replacePrevious = true;
                    }
                }
            }
            //System.out.println(newBlocks);
            //System.out.println("Previous blocks # " + blocks.size() + " new is " + newBlocks.size());
            blocks = newBlocks;
            inBlockMap = newInBlockMap;
        }

//        for(List<Long> b : blocks){
//            System.out.println("Final block " + b);
//        }
        Multigraph collapsed = new BaseMultigraph();

        // Each block is a node
        List<Long> b;
        for (int idx = 0; idx < blocks.size(); idx++) {
            b = blocks.get(idx);
            for (Long node : b) {
                if (node < 0) {
                    continue;
                }

                if (!collapsed.containsVertex(new Long(idx))) {
                    collapsed.addVertex(new Long(idx));
                }

                //Replicate edge versus each block
                Collection<Edge> outE = toCollapse.outgoingEdgesOf(node);
                List<Edge> insertedEdge = new ArrayList<>(outE.size());

                for (Edge e : outE) {
                    if (e.getLabel() > 0) {
                        Long dest = new Long(inBlockMap.get(e.getDestination()));
                        Edge newEdge = new Edge(idx, dest, e.getLabel());
                        if (!insertedEdge.contains(newEdge)) {
                            if (!collapsed.containsVertex(dest)) {
                                collapsed.addVertex(dest);
                            }
                            collapsed.addEdge(newEdge);
                            insertedEdge.add(newEdge);
                        }
                    }
                }
                break;
            }
        }
        return collapsed;
    }

    /**
     * Checks if this SimulatedQuery contains a simulation with the correct
     * mapping
     *
     * @return
     */
    public boolean isSimulation() {
        Multigraph simulation = null;
        //Try to build the graph, if this doesn't work we have not a complete mapping
        try {
            simulation = this.buildMatchedGraph();
        } catch (IllegalStateException e) {
            return false;
        }

        //System.out.println("\n\n\t" + simulation.edgeSet());

        HashMap<Long, Integer> inBlockMap = computeSimulationPartition(simulation);
        if (inBlockMap == null) {
            return false;
        }
        // FOr all nodes in the query it checks wheter is part of a correct mapping
        for (Long qNode : this.query.vertexSet()) {
            Integer idBlock = inBlockMap.get(-1 * qNode);
            for (Long sNode : this.mapOf(qNode)) {
                if (inBlockMap.get(sNode) == null) {
                    return false;
                }
                if (!inBlockMap.get(sNode).equals(idBlock)) {
                    return false;
                }
            }
        }

        ///Do the check
        return inBlockMap.values().size() > 0;
    }

    /**
     * Checks if this SimulatedQuery contains a simulation with the correct
     * mapping
     *
     * @param toCheck
     * @return
     */
    public SimulatedAnswer computeSimulation(Multigraph toCheck) {
        SimulatedAnswer result = new SimulatedAnswer(this.query);

        //We need a graph
        if (toCheck == null) {
            throw new IllegalArgumentException("Graph cannot be null");
        }

        HashMap<Long, Integer> inBlockMap = computeSimulationPartition(toCheck, false);
        if (inBlockMap == null) {
            //error("In block map is null");
            return null;
        }

        // given the inBlock map build the respective simualtion
        // For each block id find the respective query nodes
        // More than one could be there
        HashMap<Integer, List<Long>> reverseMap = new HashMap<>();

        // Ausiliary
        for (Long qNode : this.query.vertexSet()) {
            Integer idBlock = inBlockMap.get(-1 * qNode);
            if (!reverseMap.containsKey(idBlock)) {
                reverseMap.put(idBlock, new ArrayList<>());
            }
            reverseMap.get(idBlock).add(qNode);
        }

//        for (Long d  : inBlockMap.keySet()) {
//            error( d + "   -> "  + inBlockMap.get(d));
//
//        }

        // Map node to node
        for (Long node : inBlockMap.keySet()) {
            if (node > 0) {
                int nodeBlock = inBlockMap.get(node);
                if (!reverseMap.containsKey(nodeBlock)) {
                    //error("reverse map does not contain nodeBlock "+ nodeBlock + " for " + node);
//                    try {
//                        GraphUtils.exportGraphToVIZ("/tmp/" + "QUERY_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()), query, ""+this.query.edgeSet(), false);
//                        GraphUtils.exportGraphToVIZ("/tmp/" + "GRAPH_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()), toCheck, ""+toCheck.edgeSet(), false);
//                        info("SAVED DUMP FILE");
//                    } catch (LoadException ex) {
//                        error("COULD NOT SAVE FILE");
//                        error(ex.getMessage());
//                    }

                    return null;
                }
                for (Long q : reverseMap.get(nodeBlock)) {
                    result.map(q, node);
                }
            }
        }

       // Map edges
        // This is really approximative
        // but given how things works it is not important for it to be precise
        for (Edge gE : toCheck.edgeSet()) {
            if (inBlockMap.containsKey(gE.getSource())) {
                for (Long candidate : reverseMap.get(inBlockMap.get(gE.getSource()))) {
                    for (Edge qE : query.outgoingEdgesOf(candidate)) {
                        if (qE.getLabel().equals(gE.getLabel()) && Objects.equals(inBlockMap.get(gE.getDestination()), inBlockMap.get(-1 * qE.getDestination()))) {
                            result.map(qE, gE);
                        }
                    }
                }
            }

        }

        // the simulated query
        //debug("Returning solution with #edges " + result.getUsedEdgesIDs().size());
        return result;
    }

    /**
     * Partition the graph in the equivalence classes that constitute the
     * simulation relation
     *
     * @param toCheck
     * @return the mapping from each node to the equivalence class ID
     */
    public HashMap<Long, Integer> computeSimulationPartition(Multigraph toCheck) {
        return computeSimulationPartition(toCheck, true);
    }

    /**
     * Partition the graph in the equivalence classes that constitute the
     * simulation relation
     *
     * @param toCheck
     * @param strict terminate early if the graph is not a simulation
     * @return the mapping from each node to the equivalence class ID
     */
    public HashMap<Long, Integer> computeSimulationPartition(Multigraph toCheck, boolean strict) {
        //Data Structures
        List<List<Long>> blocks = new ArrayList<>(); // this is PI - the set of partitions
        List<List<Long>> newBlocks; // this is PI for the next iteration
        ArrayList<Long> labels = new ArrayList<>(); // The labels of the query
        TreeSet<Long> queryNodes = new TreeSet<>(); // the nodes of the query
        HashMap<Long, Integer> inBlockMap = new HashMap<>(); // In which block is each node?
        HashMap<Long, Integer> newInBlockMap; // In which block is each node? for the new step

        // The simulation graph: will contain two unconneted graphs
        // The query (with negative indexes) and the graph that shouls simulate it
        boolean changed = true;
        Multigraph graph = new BaseMultigraph(toCheck.numberOfEdges());
        for (Long v : toCheck.vertexSet()) {
            graph.addVertex(v);
        }

        for (Edge e : toCheck.edgeSet()) {
            graph.addEdge(e);
        }

        //Get the labes of the query
        for (Edge e : this.query.edgeSet()) {
            labels.add(e.getLabel());
        }

        //Assign a temporary id to querynodes just to avoid confusion
        //Since they are temporary, better them be negative
        List<Long> pr = new ArrayList<>(this.getUsedNodes().size() + this.query.vertexSet().size() + 1);
        //List<Long> prEmpty = new ArrayList<>(this.getUsedNodes().size() + this.query.vertexSet().size() + 1);
        for (Long node : this.query.vertexSet()) {
            if(!pr.contains(-1*node)){// && !prEmpty.contains(-1*node)){
                if(!strict && query.outDegreeOf(node) == 0){
                    //prEmpty.add(-1 * node);
                    //inBlockMap.put(-1 * node, 0);
                } else {
                    //When we consider the splits we only use node from the query
                    //Only node from the query drive the split
                    queryNodes.add(-1 * node);
                }
                pr.add(-1 * node);
                inBlockMap.put(-1 * node, 0);


                //When we consider the splits we only use node from the query
                //Only node from the query drive the split
                //queryNodes.add(-1 * node);
            }
        }

        //Add the edges of the "fake query" to the simulation graph
        for (Edge e : this.query.edgeSet()) {
            //Edge ne = new Edge(queryNodeMap.get(e.getSource()), queryNodeMap.get(e.getDestination()), e.getLabel());
            Edge ne = new Edge(-1 * e.getSource(), -1 * e.getDestination(), e.getLabel());
            graph = ExemplarAnswer.smartAddEdge(graph, ne, false);
        }

        // Add also the nodes in the simulated query to the initial block
        for (Long node : graph.vertexSet()) {
            if(!pr.contains(node)) { // && !prEmpty.contains(node)){
                if(graph.outDegreeOf(node) == 0){
                    //prEmpty.add(node);
                    //inBlockMap.put(node, 0);
                }
                pr.add( node);
                inBlockMap.put(node,0);
            }
        }


        //The initial partiion contain only the initilal block
        //blocks.add(prEmpty);
        blocks.add(pr);


        //System.out.println(" " + blocks);
        // The loop - cotinue since no more splits are done
        while (changed) {
            changed = false;
            newBlocks = new ArrayList<>();
            newInBlockMap = new HashMap<>();
            // Try to split each block
            for (List<Long> block : blocks) {
                // Cicle each label
                boolean replacePrevious = false;
                // if the block has only one node the mapping is failed in any case
                if (strict && block.size() == 1) {
                    return null;
                }
                List<Long> splitters = CollectionUtilities.intersect(block, queryNodes);
                //If there ar no splitters this block will fail the simulation
                if (splitters.isEmpty()) {
                    if (strict) {
                        return null;
                    }
                }

                for (Long label : labels) {
                    // Extract the a-labelled transition FROM states in block
                    //Try to split the block
                    List<List<Long>> tempBlocks = split(block, label, splitters, inBlockMap, graph, strict);

                    //Replace previous means that we already added this very block
                    if (replacePrevious) {
                        newBlocks.remove(newBlocks.size() - 1);
                    }

                    //If we splitted we need to change the mapping of blocks...
                    //For each block then (they are at most 2)
                    for (List<Long> b : tempBlocks) {
                        // We re-Map each node
                        // Update reachability
                        for (Long node : b) {
                            //The current node is in the block with id the current size (before adding)
                            newInBlockMap.put(node, newBlocks.size());
                        }
                        //Now we can add this new block
                        newBlocks.add(b);
                    }

                    if (tempBlocks.size() > 1) {
                        changed = true;
                        break;
                    } else {
                        replacePrevious = true;
                    }
                }
            }

            //System.out.println(" " + newBlocks);

            blocks = newBlocks;
            inBlockMap = newInBlockMap;
        }

        return inBlockMap;

    }

    private static List<List<Long>> split(List<Long> block, Long label, List<Long> splittersQueryNodes, HashMap<Long, Integer> inBlocks, Multigraph simulation, boolean strict) {
        List<Long> b1 = new ArrayList<>(block.size()), b2 = new ArrayList<>(block.size());
        List<List<Long>> output = new ArrayList<>(3);
        //Choose S

        if(splittersQueryNodes.isEmpty()){
            output.add(block);
            return output;
        }

        Long s = splittersQueryNodes.iterator().next();
        //if (!block.contains(s)) {
        //  throw new IllegalStateException("What have I done?");
        //}
        //S goes here
        //System.out.println("Splittin node is " + s);
        b1.add(s);

        //Outgoin of s
        Collection<Edge> sOut = simulation.outgoingEdgesOf(s);
        // what can S reach?
        List<Integer> sReach = new ArrayList<>(inBlocks.size() + 1);
        //System.out.println("LABEL "+ label);
        if (sOut != null) {
            for (Edge se : sOut) {
                /// Only with the given label
                if (se.getLabel().equals(label)) {
                    sReach.remove(inBlocks.get(se.getDestination())); // avoid duplicates // YES this is wrong
                    sReach.add(inBlocks.get(se.getDestination()));
                }
            }
        }
        //System.out.println(sReach);
        // For each state in B
        for (Long t : block) {
            if (!t.equals(s)) {
                // if s and t can reach the same set of blocks in π via a-labelled transitions

                //Outgoin of t
                Collection<Edge> tOut = simulation.outgoingEdgesOf(t);
                if(t.equals(6L)){
                 //System.out.println(t + " :  " + tOut);
                }
                // what can T reach?
                List<Integer> tReach = new ArrayList<>(inBlocks.size() + 1);
                if (tOut != null) {
                    for (Edge te : tOut) {
                        /// Only with the given label
                        if (te.getLabel().equals(label)) {
                            tReach.remove(inBlocks.get(te.getDestination())); // avoid duplicates // YES this is wrong
                            tReach.add(inBlocks.get(te.getDestination()));
                            //                        System.out.println(te + " goes in " +inBlocks.get(te.getDestination()));
                        }
                    }
                }
                if(!strict && t>0){
                 //   tReach.add(0);
                }
                //Do they reach the same block(s)?
                //strict= false;

                if ( ((strict || t < 0 )  && compareBlockSets(tReach, sReach)) || ((!strict && t >0) && subsetCompareBlockSets(sReach, tReach)) ) { //.equals(
                //if ( (sReach.size() > 0 && tReach.size() > 0) && ( t < 0   && compareBlockSets(tReach, sReach)) || (t >0 && subsetCompareBlockSets(sReach, tReach)) ) { //.equals(
                //if (  compareBlockSets(tReach, sReach) ) { //.equals(
                    b1.add(t);
                } else {
                    //System.out.println(t+ " "+tReach + " <>   " + s+ " " + sReach  +  "  WITH L: " + label);
                    b2.add(t);
                }

            }

        }

        output.add(b1);
        if (!b2.isEmpty()) {
            output.add(b2);
        }

        //System.out.println(output.size());
        return output;
    }

    /**
     * This comparison checks two lists, and gives true if they contain the same
     * elements
     * @param b1
     * @param b2
     * @return
     */
    private static boolean compareBlockSets(List<Integer> b1, List<Integer> b2) {
        if (b1.size() == b2.size()) {
            for (Integer i : b1) {
                if (!b2.contains(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    /**
     * This comparison checks two lists, and gives true if the first is subset of the same
     * @param b1
     * @param b2
     * @return
     */
    private static boolean subsetCompareBlockSets(List<Integer> b1, List<Integer> b2) {
//        System.out.print("b1: " + b1);
//        System.out.println(" b2: " + b2);
        if (b1.size() <= b2.size()) {
            for (Integer i : b1) {
                if (!b2.contains(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}

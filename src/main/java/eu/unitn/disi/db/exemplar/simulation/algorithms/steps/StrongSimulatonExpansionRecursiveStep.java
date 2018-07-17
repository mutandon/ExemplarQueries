/*
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
package eu.unitn.disi.db.exemplar.simulation.algorithms.steps;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.algorithms.UndirectedUnweightedDiameter;

import eu.unitn.disi.db.exemplar.simulation.core.SimulatedAnswer;
import eu.unitn.disi.db.exemplar.core.algorithms.steps.GraphSearchStep;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class StrongSimulatonExpansionRecursiveStep extends GraphSearchStep<SimulatedAnswer> {
    //  * update this object list of visited nodes to be skipped later
    //private final HashSet<Long> listed = new HashSet<>();
 
    private int maxDiameter = 0;
     
    public StrongSimulatonExpansionRecursiveStep(int threadNumber, Iterator<Long> kbConcepts, Multigraph query, Multigraph targetSubgraph, int limitComputation, boolean skipSave, Set<Long> whiteList, int memoryLimit, int maxDiameter) {
        super(threadNumber, kbConcepts, query, targetSubgraph, limitComputation, skipSave, whiteList, memoryLimit);
        this.maxDiameter = maxDiameter;
    }

    @Override
    public Collection<SimulatedAnswer> call() throws Exception {
        SimulatedAnswer temp;

        LinkedHashSet<SimulatedAnswer> relatedQueries = new LinkedHashSet<>(3500);
        if(maxDiameter<1){
            UndirectedUnweightedDiameter dimComp = new UndirectedUnweightedDiameter();
            dimComp.setGraph(query);
            dimComp.compute();
            this.maxDiameter = dimComp.getDiameter();
            debug("Diameter %d ", this.maxDiameter);
        }
        

        
        while (graphNodes.hasNext()) {            
            Long node = graphNodes.next();
            if(whiteList != null && !whiteList.contains(node)){
                continue;
            }
            //watch.start();
            try {
                // Get the diameter of the query
                //Find the Ball with this node as center
                List<Edge> connectedComponent = findComponentEdges(node, this.maxDiameter, this.graph);
                //debug("Seed %s : %s", node, connectedComponent.toString());
                // Check if the components is actually containing something
                if ( connectedComponent.size()  > 0) {
                    //if(connectedComponent.size()> 500){
                    //    debug("Node  %d  balls size %d", node, connectedComponent.size());
                    //}
                    // Put the multigraph in a query
                    //temp = relatedQuery.computeSimulation(connectedComponent);
                    temp = strongSimulation(query, connectedComponent, node);
                    connectedComponent.clear();
                        //
                    if(skipSave){
                        continue;
                    }
                                    
                    
                    if (temp != null) {
                        // Check if this component can simulate the query.
                        if (!temp.isMappingComplete()) {
                            error("Incomplete simulation from node %s; \nquery %s \nresult %s ", node, this.query.toString(), temp.buildMatchedGraph().toString());
                        } else if(!relatedQueries.contains(temp)) {
                            relatedQueries.add(temp);
                        }

                        //debug("Got %d for node %s", relatedQueries.size(), node);
                        if (limitComputation > 0 && relatedQueries.size() > limitComputation) {
                            warn("More than " + limitComputation + " partial simulated results - going to break here");
                            break;                            
                        }

                        if (Thread.currentThread().isInterrupted()) {
                            warn("The thread has received a killing signal");
                            throw new InterruptedException("The computation has been interrupted!");
                        }
                        Runtime runtime = Runtime.getRuntime();                    
                        if (this.memoryLimit > 0 && (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.) > this.memoryLimit) {
                            warn("Memory limit reached after %s queries, memory used is: %.2fMb, free memory: %.2fMb. Returning the answers computed so far", relatedQueries.size(), (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.), runtime.freeMemory()/(1024*1024.));
                            this.memoryExhausted = true; 
                            throw new InterruptedException("The memory is exhausted");
                        }

                    }

                }
            } catch (OutOfMemoryError e) {

                error("Memory exausted, so we are returning something but not everything.", e);
                this.memoryExhausted=true;
                //System.gc();
                return relatedQueries;
            }
            /*watch.stop();
            if (watch.getElapsedTimeMillis() > WARN_TIME) {
                info("Computation %d [%d] took %d ms", threadNumber, Thread.currentThread().getId(), watch.getElapsedTimeMillis());
            }
            watch.reset();*/
        }

        return relatedQueries;
    }

    
    private Set<Long> findComponentNodes(Long node, int diameter, Multigraph searchSpace ) {        
            return findComponentNodes(node, diameter, searchSpace, this.whiteList);               
    }
    
    /**
     * Given a starting node from the graph, finds the complete connected
     * component (Ball)
     *
     * @param node
     * @param diameter
     * @param searchSpace
     * @return the connected component starting from this node
     */
    private Set<Long> findComponentNodes(Long node, int diameter, Multigraph searchSpace, Set<Long> allowed ) {
        
        LinkedList<Long> toVisit = new LinkedList<>();
        HashSet<Long> component = new HashSet<>(searchSpace.numberOfNodes()/2);

        Long next = null;
        Long toAdd;
        final Long fake = Long.MIN_VALUE;
        int level = 0;
        component.add(node);
        toVisit.add(node);
        toVisit.add(fake);
        //listed.add(node);
        boolean stop = false;
        Edge e;
        Iterator<Edge> aEdges;
        while (!stop && (next = toVisit.pollFirst()) != null) {
            if(Objects.equals(next, fake)){
                level++;
                if(toVisit.isEmpty() || level >= diameter){
                    stop = true;
                    break;
                }
                toVisit.add(fake);
                // else
                continue;
            }
            
            
            for (int i = 0; i < 2; i++) {
                aEdges = i == 0 ? searchSpace.incomingEdgesIteratorOf(next) : searchSpace.outgoingEdgesIteratorOf(next);
                while (aEdges.hasNext()) {
                    e = aEdges.next();
                    toAdd = i == 0 ? e.getSource() : e.getDestination();
                    if(allowed != null && !allowed.contains(toAdd)){
                        continue;
                    }
                    
                    if(!component.contains(toAdd)){
                       toVisit.add(toAdd);
                       component.add(toAdd);
                    }
                }
            }                        
        }

        return component;
    }
     
    
    /**
     * Given a starting node from the graph, finds the complete connected
     * component (Ball)
     *
     * @param node
     * @param diameter
     * @param searchSpace
     * @return the connected component starting from this node
     */
    public List<Edge> findComponentEdges(Long node, int diameter, Multigraph searchSpace) {
        Set<Long> component = findComponentNodes(node, diameter, searchSpace);
        ArrayList<Edge> compEdges = new ArrayList<>(component.size()*4/3);
        Iterator<Edge> aEdges = searchSpace.edgesIterator();
        Edge e;
        while (aEdges.hasNext()) {
            e = aEdges.next();
            if (component.contains(e.getSource()) && component.contains(e.getDestination())) {
                compEdges.add(e);
            }
        }
        return compEdges;
    }
    
    /**
     * Given a starting node from the graph, finds the complete connected
     * component (Ball)
     *
     * @param node
     * @param diameter
     * @param searchSpace
     * @return the connected component starting from this node
     */
    public Multigraph findComponent(Long node, int diameter, Multigraph searchSpace) {
        Multigraph component = new BaseMultigraph();
        Set<Long> componentNodes = findComponentNodes(node, diameter, searchSpace);
        Iterator<Edge> aEdges = searchSpace.edgesIterator();
        Edge e;
        while (aEdges.hasNext()) {
            e = aEdges.next();
            if (componentNodes.contains(e.getSource()) && componentNodes.contains(e.getDestination())) {
                component.addEdge(e);
            }
        }

        return component;
    }

    


    private SimulatedAnswer strongSimulation(Multigraph query, Collection<Edge> ballEdges, Long center) throws AlgorithmExecutionException{
        SimulatedAnswer solution = new SimulatedAnswer(query);
        Multigraph expandedBall = new BaseMultigraph();
        Multigraph expandedQuery  = new BaseMultigraph();
        // Nodes have different IDs but repeated labels
        HashMap<Long, Long> ballNodeLabels = new HashMap<>();
        HashMap<Long, Long> queryNodeLabels = new HashMap<>();

        HashMap<Long,Edge> ballExpansionMapping = new HashMap<>();
        HashMap<Long,Edge> queryExpansionMapping = new HashMap<>();
        
        // Inverted index
        HashMap<Long, Set<Long>> ballReverseNodeLabels = new HashMap<>();
        HashMap<Long, Set<Long>> queryReverseNodeLabels = new HashMap<>();
        
        // We need a new id for new nodes
        Long nodeInc =0L;
        //All real nodes have the same label
        Long genericLabel = Long.MIN_VALUE;

        nodeInc = replaceEdgesWithNodes(ballEdges, expandedBall, ballNodeLabels,  nodeInc, ballReverseNodeLabels,genericLabel, ballExpansionMapping ); // ballEdgeNodesIDs
        replaceEdgesWithNodes(query.edgeSet(), expandedQuery, queryNodeLabels, nodeInc, queryReverseNodeLabels,genericLabel, queryExpansionMapping ); //queryEdgeNodesIDs

        //Compute dual simulation
        // This is the Simulation "Relation"
        HashMap<Long, Set<Long>> simRel = new HashMap<>();

        HashSet<Long> simulating;
        for(Long node : expandedQuery.vertexSet()){
            //query nodes map initially to all nodes
            simulating = new HashSet<>();
            Set<Long> toAdd = ballReverseNodeLabels.get(queryNodeLabels.get(node));
            if(toAdd == null){
                // There are no nodes in the ball to map this query node
                //debug("NO RELATIONSHIP NODE %s", queryNodeLabels.get(node));
                return null;
            }
            simulating.addAll(toAdd);
            simRel.put(node, simulating);
        }

        boolean isChanged = true;

        while(isChanged){
            isChanged = false;

            for(Long v :expandedQuery.vertexSet()){
              //For each v v' in Eq
              for(Edge qe : expandedQuery.outgoingEdgesOf(v)){
                //for(Edge qe : expandedQuery.edgeSet()){
                    //Long v = qe.getSource();
                    Long v_prime = qe.getDestination();
                    // For each  u in sim(v)
                    List<Long> toRemove = new ArrayList<>();
                    for(Long u : simRel.get(v)){
                        boolean found = false;
                        //Exist  one edge (u, u')  u' \in sim(v')
                        for(Edge be : expandedBall.outgoingEdgesOf(u)){
                            /// is u' \in sim(v')
                            // u' = be.getDestination()
                            if(simRel.get(v_prime).contains(be.getDestination())){
                                found = true;
                                break;
                            }
                        }
                        if(!found){
                            toRemove.add(u);
                            isChanged = true;
                        }
                    }
                    simRel.get(v).removeAll(toRemove);
                }

                for(Edge qe : expandedQuery.incomingEdgesOf(v)){
                //for(Edge qe : expandedQuery.edgeSet()){
                    //Long v = qe.getDestination();
                    Long v_prime = qe.getSource();
                    // For each  u in sim(v)
                    List<Long> toRemove = new ArrayList<>();
                    for(Long u : simRel.get(v)){
                        boolean found = false;
                        //Exist  one edge (v, v')  v' \in sim(u')
                        for(Edge be : expandedBall.incomingEdgesOf(u)){
                            /// is v' \in sim(u')
                            // v' = be.getSource()
                            if(simRel.get(v_prime).contains(be.getSource())){
                                found = true;
                                break;
                            }
                        }
                        if(!found){
                            toRemove.add(u);
                            isChanged = true;
                        }
                    }
                    simRel.get(v).removeAll(toRemove);
                }

                if(simRel.get(v).isEmpty()){
                    //debug("INVALID BECAUSE %d HAS NO SIM-REL", v);
                    return null;
                }

            }
        }

        // Check if ball is valid
        boolean valid = false;
        HashSet<Long> validNodes = new HashSet<>(); // Nodes that will be in the final solution
        for(Long u :  expandedQuery.vertexSet()){
            valid = valid || simRel.get(u).contains(-1*center);
            validNodes.addAll(simRel.get(u));
        }
        if(!valid){
    //        debug(("INVALID BECAUSE %d IS NOT IN THE SIMULATION"), center);
            return null;
        }


        // Extract MaxPG <- this is because it should be only the  connected part
        Set<Long> expandedBallNodes = findComponentNodes(-1*center, Integer.MAX_VALUE, expandedBall, validNodes);

        
        for(Long u :  queryExpansionMapping.keySet()){
            //if(u > 0L){ // Is an edge node!
                
                Edge queryEdge = queryExpansionMapping.get(u);
                
                for(Long node : simRel.get(u)){ // Matching edges
                    if(!expandedBallNodes.contains(node)){
                        continue;
                    }
                    // Good edges
                    if( expandedBall.incomingEdgesOf(node) == null  || expandedBall.incomingEdgesOf(node).size() != 1){
                        int num = expandedBall.incomingEdgesOf(node) == null ? 0 : expandedBall.incomingEdgesOf(node).size();
                        throw new AlgorithmExecutionException(" Expanded Node %s has wrong matching source, found %d nodes ", node, num );
                    }
                    if( expandedBall.outgoingEdgesOf(node) == null || expandedBall.outgoingEdgesOf(node).size() != 1){
                        int num = expandedBall.outgoingEdgesOf(node) == null  ? 0 : expandedBall.outgoingEdgesOf(node).size();
                        throw new AlgorithmExecutionException(" Expanded Node %s has wrong matching destination found %d nodes ", node,  num );
                    }

                    Edge ballEdge = ballExpansionMapping.get(node);
                    
                    if(!Objects.equals(queryEdge.getLabel(), ballEdge.getLabel()) ) {
                        //error("R U Serius? ...you mapped %d to %d , is this a joke?", queryNodeLabels.get(u),  ballNodeLabels.get(node)   );
                        throw new AlgorithmExecutionException("Wrong Edge mapping");
                    }
                    if(validNodes.contains(-1*ballEdge.getSource())  && validNodes.contains(-1*ballEdge.getDestination()) ){
                        
                        //Mapping
                        solution.map(queryEdge.getSource(), ballEdge.getSource());
                        solution.map(queryEdge.getDestination(), ballEdge.getDestination());
                        solution.map(queryEdge, ballEdge);
                    } else {
                        //error("So, right. Let's talk about it... my dear %s, you should be there... I mean, seriously is at least %s there? %s and %s", source, destination, validNodes.contains(source) ? "yes" : "no",   validNodes.contains(destination) ? "yes" : "no"  );
                        throw new AlgorithmExecutionException("Illegal Structure of the graph");
                        //return null;
                    }
                }
            //}
        }
        
        if(!solution.isMappingComplete()){
//            error("WAS INCOMPLETE");
            return null;
        }
        
//        Multigraph f = solution.buildMatchedGraph();
//        expandedBallNodes = findComponentNodes(f.iterator().next(), Integer.MAX_VALUE, solution.buildMatchedGraph());
//        if(expandedBallNodes.size()!=f.numberOfNodes()){
//            error("WAS DISCONNECTED %s /  %s ", expandedBallNodes.size(), f.numberOfNodes());
//            return null;
//        }
        //// BUILD RELATED
        // return
        return solution;

    }

    /**
     * 
     * @param toConvert
     * @param converted
     * @param nodeLabels
     * @param startID   we need to create new IDs for nodes we add
     * @param reverseNodeLabels
     * @param expansionMapping maps ID of new nodes to the original edge they model
     * @param genericLabel
     * @return 
     */
    private Long replaceEdgesWithNodes(Collection<Edge> toConvert, Multigraph converted, HashMap<Long, Long> nodeLabels,  Long startID, HashMap<Long, Set<Long>> reverseNodeLabels, Long genericLabel, HashMap<Long,Edge> expansionMapping){ //HashMap<String, Long> edgeNodesIDs ,

        // Expand graph to labeled nodes only
        for (Edge e : toConvert){
            //We have the original nodes negative to recognize them
            Long src = -1*e.getSource();
            Long dest = -1*e.getDestination();
            Long label = e.getLabel();

            //Create a new node ID for the edge
            startID++;
            // original nodes will be negative
            // new nodes will be the only positive
            expansionMapping.put(startID, e);

            if(!nodeLabels.containsKey(src)){
                nodeLabels.put(src,genericLabel );
                safePut(genericLabel, src, reverseNodeLabels);
            }

            if(!nodeLabels.containsKey(dest)){
                nodeLabels.put(dest, genericLabel);
                safePut(genericLabel, dest, reverseNodeLabels);
            }

            nodeLabels.put(startID, label); // This node label is the edge label
            safePut(label, startID, reverseNodeLabels);
            //Identify this node please
            //edgeNodesIDs.put(e.getId(), startID);

            //Now put the nodes in the graph and connect all of them
            converted.addVertex(src);
            converted.addVertex(dest);
            converted.addVertex(startID);
            converted.addEdge(src, startID, 0L);  // New graph has no lables
            converted.addEdge(startID, dest, 0L);  // New graph has no lables
        }
        return startID;
    }


    private void safePut(Long key, Long value, HashMap<Long, Set<Long>> map){

        Set<Long> values = map.get(key);

        if(values == null){
            values = new HashSet<>();
            map.put(key, values);
        }

        values.add(value);
    }

}

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

import eu.unitn.disi.db.exemplar.simulation.core.SimulatedAnswer;
import eu.unitn.disi.db.exemplar.core.algorithms.steps.GraphSearchStep;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.Collection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class GraphSimulationRecursiveStep extends GraphSearchStep<SimulatedAnswer> {


    private final Long queryConcept;

    /**
     * 
     * @param threadNumber
     * @param kbConcepts
     * @param queryConcept
     * @param query
     * @param targetSubgraph
     * @param limitComputation
     * @param skipSave
     * @param whitelist  <- THIS IS NOT USED YET
     * @param memoryLimit 
     */
    public GraphSimulationRecursiveStep(int threadNumber, Iterator<Long> kbConcepts, Long queryConcept, Multigraph query, Multigraph targetSubgraph, int limitComputation, boolean skipSave,Set<Long> whitelist , int memoryLimit) {
        super(threadNumber,kbConcepts,query, targetSubgraph, limitComputation, skipSave, whitelist, memoryLimit);
        this.queryConcept = queryConcept;
    }

    @Override
    public List<SimulatedAnswer> call() throws Exception {
        SimulatedAnswer relatedQuery;
        List<SimulatedAnswer> relatedQueriesPartial = new LinkedList<>();
        Set<SimulatedAnswer> relatedQueries = new LinkedHashSet<>();

        boolean warned = false;
        //watch.start();
        while (graphNodes.hasNext()) {
            Long node = graphNodes.next();
            try {
                relatedQuery = new SimulatedAnswer(query);
                //Map the first node
                relatedQuery.map(queryConcept, node);
                relatedQueriesPartial = createQueries(query, queryConcept, node, relatedQuery, System.currentTimeMillis());
                if (relatedQueriesPartial != null) {
                    if(skipSave){
                        continue;
                    }
                    for (SimulatedAnswer s : relatedQueriesPartial) {
                        if(!relatedQueries.contains(s) && s.isSimulation()){
                            relatedQueries.add(s);
                        }
                    }

                    //debug("Got %d for node %s", relatedQueries.size(), node);
                    if (!warned  && limitComputation >0 &&  relatedQueries.size() > limitComputation) {                        
                        warn("More than " + limitComputation + " partial simulated results - going to break here");
                        break;                        
                    }
                }
                if (Thread.currentThread().isInterrupted()) {
                    warn("The thread has received a killing signal");
                    throw new InterruptedException("The computation has been interrupted!");
                }
                Runtime runtime = Runtime.getRuntime();                    
                if (this.memoryLimit > 0 && (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.) > this.memoryLimit) {
                    warn("Memory limit reached, memory used is: %.2fMb, free memory: %.2fMb. Returning the answers computed so far", (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.), runtime.freeMemory()/(1024*1024.));
                    this.memoryExhausted = true; 
                    throw new InterruptedException("The memory is exhausted");
                }
                
            } catch (OutOfMemoryError E) {
                if (relatedQueriesPartial != null) {
                    relatedQueriesPartial.clear();
                }
                error("Memory exausted, so we are returning something but not everything.");
                //System.gc();
                return new LinkedList<>(relatedQueries);
            }

        }

        return new LinkedList<>(relatedQueries);
    }

    /**
     * Given a query, a starting node from the query, and a node from the
     * knowledgeBase , tries to build up a related query
     *
     * @param query
     * @param queryNode
     * @param graphNode
     * @param relatedQuery
     * @param curr
     * @return
     * @throws java.lang.InterruptedException
     */
    public List<SimulatedAnswer> createQueries(Multigraph query, Long queryNode, Long graphNode, SimulatedAnswer relatedQuery, Long curr) throws InterruptedException {
        // Initialize the queries set
        //Given the current situation we expect to build more than one possible related query
        LinkedList<SimulatedAnswer> relatedQueries = new LinkedList<>();
        relatedQueries.add(relatedQuery);

        // The graphEdges exiting from the query node passed
        Collection<Edge> queryEdgesOut = query.outgoingEdgesOf(queryNode);
        // The graphEdges entering the query node passed
        Collection<Edge> queryEdgesIn = query.incomingEdgesOf(queryNode);

        // The graphEdges in the KB exiting from the mapped node passed
        Collection<Edge> graphEdgesOut = graph.outgoingEdgesOf(graphNode);
        // The graphEdges in the KB entering the mapped node passed
        Collection<Edge> graphEdgesIn = graph.incomingEdgesOf(graphNode);

        // Null handling
        queryEdgesIn = queryEdgesIn == null ? new HashSet<>() : queryEdgesIn;
        queryEdgesOut = queryEdgesOut == null ? new HashSet<>() : queryEdgesOut;
        graphEdgesIn = graphEdgesIn == null ? new HashSet<>() : graphEdgesIn;
        graphEdgesOut = graphEdgesOut == null ? new HashSet<>() : graphEdgesOut;

        //Optimization: if the queryEdges are more than the kbEdges, we are done, not mappable!
        if ((!queryEdgesIn.isEmpty() && graphEdgesIn.isEmpty()) || (!queryEdgesOut.isEmpty() && graphEdgesOut.isEmpty())) {
            return null;
        }

        //All non mapped graphEdges from the query are put in one set
        Set<Edge> queryEdges = new HashSet<>();

        for (Edge edgeOut : queryEdgesOut) {
            if (!relatedQuery.hasMapped(edgeOut)) {
                queryEdges.add(edgeOut);
            }
        }

        for (Edge edgeIn : queryEdgesIn) {
            if (!relatedQuery.hasMapped(edgeIn)) {
                queryEdges.add(edgeIn);
            }
        }

        queryEdgesIn = null;
        queryEdgesOut = null;
        Runtime runtime = Runtime.getRuntime();
        if (this.memoryLimit > 0 && (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.) > this.memoryLimit) {
            warn("Memory limit reached, memory used is: %.2fMb, free memory: %.2fMb. Returning the answers computed so far", (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.), runtime.freeMemory()/(1024*1024.));
            this.memoryExhausted = true; 
            throw new InterruptedException("The memory is exhausted");
        }

        
        //Look if we can map all the outgoing/ingoing graphEdges of the query node
        for (Edge queryEdge : queryEdges) {

            LinkedList<SimulatedAnswer> newRelatedQueries = new LinkedList<>();
            LinkedList<SimulatedAnswer> toTestRelatedQueries = new LinkedList<>();

            for (SimulatedAnswer current : relatedQueries) {
                if (current.hasMapped(queryEdge)) {
                    newRelatedQueries.add(current);
                } else {
                    toTestRelatedQueries.add(current);
                }
            }

            // If all candidated have this QueryEdge mapped, go to next
            if (toTestRelatedQueries.isEmpty()) {
                relatedQueries = newRelatedQueries;
                continue;
            }

            // The label we are looking for
            Long label = queryEdge.getLabel();

            //is it isIncoming or outgoing ?
            boolean isIncoming = queryEdge.getDestination().equals(queryNode);

            List<Edge> graphEdges;
            // Look for graphEdges with the same label and same direction as the one from the query
            if (isIncoming) {
                graphEdges = findEdges(label, graphEdgesIn);
            } else {
                graphEdges = findEdges(label, graphEdgesOut);
            }

            // Do we found any?
            if (graphEdges.isEmpty() && isIncoming) {
                continue;
            } else if (graphEdges.isEmpty()) {
                // If we cannot map OUTGOING graphEdges, this path is wrong
                return null;
            } else {
                // reset, we do not want too many duplicates
                //relatedQueries = new LinkedList<>();

                //Cycle through all the possible graphEdges options,
                //they would be possibly different related queries
                for (Edge graphEdge : graphEdges) {
                    //Cycle through all the possible related queries retrieved up to now
                    //A new related query is good if it finds a match
                    for (SimulatedAnswer tempRelatedQuery : toTestRelatedQueries) {
                        //if (tempRelatedQuery.isUsing(graphEdge)) {
                        //Ok this option is already using this edge, it could map two query edges
                        //we cannot discard it
                        //continue;
                        //}
                        //Otherwise this edge can be mapped to the query edge if all goes well
                        SimulatedAnswer newRelatedQuery = tempRelatedQuery.getClone();

                        //check nodes similarity
                        //double nodeSimilarity = 0;
                        //if (isIncoming) {
                        //    nodeSimilarity = RelatedQuerySearch.conceptSimilarity(queryEdge.getSource(), graphEdge.getSource());
                        //} else {
                        //    nodeSimilarity = RelatedQuerySearch.conceptSimilarity(queryEdge.getDestination(), graphEdge.getDestination());
                        //}
                        //If the found edge peudo-destination is similar to the query edge pseudo-destination
                        //if (nodeSimilarity > RelatedQuerySearch.MIN_SIMILARITY) {
                        //The destination if outgoing the source if isIncoming
                        Long queryNextNode;
                        Long graphNextNode;
                        if (isIncoming) {
                            queryNextNode = queryEdge.getSource();
                            graphNextNode = graphEdge.getSource();
                        } else {
                            queryNextNode = queryEdge.getDestination();
                            graphNextNode = graphEdge.getDestination();
                        }

                        //Is this node coeherent with the structure?
                        if (edgeMatch(queryEdge, graphEdge, newRelatedQuery)) {

                            //That's a good edge!! Add it to this related query
                            newRelatedQuery.map(queryEdge, graphEdge);

                            //Map also the node
                            newRelatedQuery.map(queryNextNode, graphNextNode);

                            //Whe are exploring in advance
                            //the query node that we are going to map
                            //Does it have queryEdges that we don't have mapped?
                            boolean needExpansion = false;
                            Collection<Edge> pseudoOutgoingEdges = query.incomingEdgesOf(queryNextNode);
                            if (pseudoOutgoingEdges.size() > 0) {
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    needExpansion = !newRelatedQuery.hasMapped(pseudoEdge) && !pseudoEdge.equals(queryEdge);
                                    if (needExpansion) {
                                        break;
                                    }
                                }
                            }

                            pseudoOutgoingEdges = query.outgoingEdgesOf(queryNextNode);
                            if (!needExpansion && pseudoOutgoingEdges.size() > 0) {
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    needExpansion = !newRelatedQuery.hasMapped(pseudoEdge) && !pseudoEdge.equals(queryEdge);
                                    if (needExpansion) {
                                        break;
                                    }
                                }
                            }

                            //Lookout! We need to check the outgoing part, if we did not already
                            if (needExpansion) {
                                // Possible outgoing branches
                                List<SimulatedAnswer> tmpRelatedQueries;
                                //Go find them!
                                tmpRelatedQueries = createQueries(query, queryNextNode, graphNextNode, newRelatedQuery, curr);
                                //Did we find any?
                                if (tmpRelatedQueries != null) {
                                    //Ok so we found some, they are all good to me
                                    //More possible related queries
                                    //They already contain the root
                                    for (SimulatedAnswer branch : tmpRelatedQueries) {
                                        //All these related queries have found in this edge their match
                                        newRelatedQueries.add(branch);
                                    }
                                }
                                // else {
                                // This query didn't find in this edge its match
                                // continue;
                                //}
                            } else {
                                //log("Complete query " + relatedQuery);
                                //this related query has found in this edge is map
                                //newRelatedQuery.map(queryNextNode, graphNextNode);
                                newRelatedQueries.add(newRelatedQuery);
                            }
                        }
                        //else {
                        //info("Edge does not match  %s   -  for %s  : %d", graphEdge.getId(), FreebaseConstants.convertLongToMid(graphNode), graphNode);
                        //}
                    }
                }
            }

            //after this cycle we should have found some, how do we check?
            if (newRelatedQueries.isEmpty()) {
                return null;
            }

            //basically in the *new* list are the related queries still valid and growing
            relatedQueries = newRelatedQueries;
        }

        return relatedQueries.size() > 0 ? relatedQueries : null;
    }

    /**
     *
     * @param queryEdge
     * @param graphEdge
     * @param r
     * @return
     */
    protected boolean edgeMatch(Edge queryEdge, Edge graphEdge, SimulatedAnswer r) {
        if (r == null) {
            throw new IllegalStateException("Edge matching over Null related query");
        }

        return (queryEdge.getLabel() == graphEdge.getLabel().longValue());
    }

    /**
     * Checks if, for a given node, it exist an <b>outgoing</b>
     * with that label and returns all the graphEdges found
     *
     * @param label the label we are looking for
     * @param graphEdges the knoweldgebase graphEdges
     * @return labeled graphEdges, can be empty
     */
    public static List<Edge> findEdges(Long label, Collection<Edge> graphEdges) {

        // Compare to the graphEdges in the KB exiting from the mapped node passed
        List<Edge> edges = new LinkedList<>();

        for (Edge Edge : graphEdges) {
            if (label == Edge.GENERIC_EDGE_LABEL || Edge.getLabel().longValue() == label) {
                edges.add(Edge);
            }
        }
        return edges;
    }

}

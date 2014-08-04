/*
 * Copyright (C) 2012 Kuzeko
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
package eu.unitn.disi.db.exemplar.core.algorithms;

import eu.unitn.disi.db.command.util.LoggableObject;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 *
 * @author Kuzeko
 */
class GraphIsomorphismRecursiveStep extends LoggableObject implements Callable<List<RelatedQuery>> {
    private Iterator<Long> kbConcepts;
    private Multigraph query;
    private Multigraph knowledgeBaseSubgraph;
    private Long queryConcept;
    private int threadNumber;
    private StopWatch wa ;
    public static final int MAX_RELATED = 5000;
    private boolean interrupt = false; 
    
    public GraphIsomorphismRecursiveStep(int threadNumber, Iterator<Long> kbConcepts, Long queryConcept, Multigraph query, Multigraph knowledgeBaseSubgraph, boolean interrupt) {
        this.kbConcepts = kbConcepts;
        this.query = query;
        this.knowledgeBaseSubgraph = knowledgeBaseSubgraph;
        this.queryConcept = queryConcept;
        this.threadNumber = threadNumber;
        this.wa = new StopWatch(StopWatch.TimeType.CPU);
        this.interrupt = interrupt; 
    }


    @Override
    public List<RelatedQuery> call() throws Exception {
        RelatedQuery relatedQuery;
        List<RelatedQuery> relatedQueriesPartial = new LinkedList<>();
        List<RelatedQuery> relatedQueries = new LinkedList<>();
        int cyclesCount = 0;
        while (kbConcepts.hasNext()) {
            Long concept = kbConcepts.next();
            wa.start();
            try {
                relatedQuery = new RelatedQueryRecursive(query);
                //Map the first concept
                relatedQuery.map(queryConcept, concept);

                relatedQueriesPartial = createQueries(query, queryConcept, concept, relatedQuery, "\t");
                if (relatedQueriesPartial != null) {
                    relatedQueries.addAll(relatedQueriesPartial);
                    if (relatedQueries.size() > MAX_RELATED) {
                        warn("More than " + MAX_RELATED + " partial isomorphic results");      
                        if (interrupt) {
                            warn("Interrupting the isomorphism expansion");
                            break;
                        }
                        //System.gc();
                        //TODO: remove DEBUG
                        //break;
                    }
                    // memento mori return relatedQueries;
                }
            } catch (OutOfMemoryError E) {
                if (relatedQueriesPartial != null) {
                    relatedQueriesPartial.clear();
                }
                error("Memory exausted, so we are returning something but not everything.");
                System.gc();
                return relatedQueries;
            }
            wa.stop();
            if(wa.getElapsedTimeMillis() > 500){
                info("Computation %d [%d] took %d ms", threadNumber, Thread.currentThread().getId(), wa.getElapsedTimeMillis());
            }
            wa.reset();
        }

        return relatedQueries;

    }

    /**
     * Given a query, a starting concept from the query, and a concept from the
     * knowledgeBase , tries to build up a related query
     *
     * @param query
     * @param queryConcept
     * @param kbConcept
     * @return
     */
    public List<RelatedQuery> createQueries(Multigraph query, Long queryConcept, Long kbConcept, RelatedQuery relatedQuery, String step) {


        // The edges exiting from the query concept passed
        Collection<Edge> queryEdgesOut = query.outgoingEdgesOf(queryConcept);
        // The edges entering the query concept passed
        Collection<Edge> queryEdgesIn = query.incomingEdgesOf(queryConcept);


        // The edges in the KB exiting from the mapped concept passed
        Collection<Edge> kbEdgesOut = knowledgeBaseSubgraph.outgoingEdgesOf(kbConcept);
        // The edges in the KB entering the mapped concept passed
        Collection<Edge> kbEdgesIn = knowledgeBaseSubgraph.incomingEdgesOf(kbConcept);

        queryEdgesIn = queryEdgesIn==null ? new HashSet<Edge>() : queryEdgesIn ;
        queryEdgesOut = queryEdgesOut==null ? new HashSet<Edge>() : queryEdgesOut ;
        kbEdgesIn = kbEdgesIn ==null ? new HashSet<Edge>() : kbEdgesIn ;
        kbEdgesOut = kbEdgesOut ==null ? new HashSet<Edge>() : kbEdgesOut ;


//        if(queryConcept.longValue()==kbConcept.longValue()){
//            loggable.debug("The same %d", queryConcept );
//        }

        //loggable.debug("Mapping %s to %s ", FreebaseConstants.convertLongToMid(queryConcept) , FreebaseConstants.convertLongToMid(kbConcept) );
//        if(checkpointNodes.keySet().contains(kbConcept)){
//            loggable.debug(checkpointNodes.get(kbConcept) + " found as matching!!  With incomings" + kbEdgesIn.size() + " and outgoing " + kbEdgesOut.size());
//        }


        //Optimization: if the queryEdges are more than the kbEdges, we are done, error!
        if (queryEdgesIn.size() > kbEdgesIn.size()) {
            //loggable.debug("incoming queryEdges %d are more than the kbEdges %d, we are done or error!",queryEdgesIn.size(), kbEdgesIn.size() );
            return null;
        }

        if (queryEdgesOut.size() > kbEdgesOut.size()) {
            //loggable.debug("outgoing queryEdges %d are more than the kbEdges %d, we are done or error!",queryEdgesOut.size(), kbEdgesOut.size() );
            return null;
        }



        //All non mapped edges from the query are put in one set
        Set<Edge> queryEdges = new HashSet<Edge>();


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


        // Initialize the queries set
        //Given the current situation we expect to build more than one possible related query
        List<RelatedQuery> relatedQueries = new ArrayList<RelatedQuery>();
        relatedQueries.add(relatedQuery);

        //info("We need to map " + queryEdges.size() + " outgoing query edges from the node "+ queryConcept + " and we have " + kbEdgesIn.size() + kbEdgesOut.size() );

        //Look if we can map all the outgoing/ingoing edges of the query concept
        //debug("Starting cycle over %d edges" , queryEdges.size() );
        for (Edge queryEdge : queryEdges) {
            //log("Trying to map the edge " + queryEdge);
            List<RelatedQuery> newRelatedQueries = new ArrayList<RelatedQuery>();

            for (RelatedQuery current : relatedQueries) {
                if (current.hasMapped(queryEdge)) {
                    newRelatedQueries.add(current);
                }
            }



            // The label we are looking for
            Long label = queryEdge.getLabel();

            //is it isIncoming or outgoing ?
            boolean isIncoming = queryEdge.getDestination().equals(queryConcept);

            List<Edge> edges;
            // Look for edges with the same label and same direction as the one from the query
            if (isIncoming) {
                edges = findEdges(label, kbEdgesIn);
            } else {
                edges = findEdges(label, kbEdgesOut);
            }
            //loggable.debug("Matching with %d edges", edges.size() );
            // Do we found any?
            if (edges.isEmpty()) {
                // If we cannot map edges, this path is wrong
                //loggable.debug("NO EDGES-Cannot map"+FreebaseConstants.convertLongToMid(queryConcept)+" to "+FreebaseConstants.convertLongToMid(kbConcept));
                return null;
            } else {
                //Cycle through all the possible edges options,
                //they would be possibly different related queries
                for (Edge kbEdge : edges) {
                    //Cycle through all the possible related queries retrieved up to now
                    //A new related query is good if it finds a match
                    for (RelatedQuery tempRelatedQuery : relatedQueries) {
                        if (tempRelatedQuery.isUsing(kbEdge)) {
                            //Ok this option is already using this edge,
                            //not a good choice go away
                            //it means that this query didn't found his match in this edge
                            continue;
                        }
                        //Otherwise this edge can be mapped to the query edge if all goes well
                        RelatedQuery newRelatedQuery = new RelatedQueryRecursive(query);
                        newRelatedQuery.setMappedConcepts(tempRelatedQuery.getMappedConcepts());
                        newRelatedQuery.setMappedEdges(tempRelatedQuery.getMappedEdges());
                        newRelatedQuery.setUsedEdges(tempRelatedQuery.getUsedEdges());
                        newRelatedQuery.setReversedMappedConcepts(tempRelatedQuery.getRevesedMappedConcepts());

                        //check nodes similarity
                        double nodeSimilarity = 0;
                        if (isIncoming) {
                            nodeSimilarity = RelatedQuery.conceptSimilarity(queryEdge.getSource(), kbEdge.getSource());
                        } else {
                            nodeSimilarity = RelatedQuery.conceptSimilarity(queryEdge.getDestination(), kbEdge.getDestination());
                        }


                        //If the found edge peudo-destination is similar to the query edge pseudo-destination
                        if (nodeSimilarity > RelatedQuery.MIN_SIMILARITY) {

                            //The destination if outgoing the source if isIncoming
                            Long queryPseudoDestination;
                            Long kbPseudoDestination;
                            if (isIncoming) {
                                queryPseudoDestination = queryEdge.getSource();
                                kbPseudoDestination = kbEdge.getSource();
                            } else {
                                queryPseudoDestination = queryEdge.getDestination();
                                kbPseudoDestination = kbEdge.getDestination();
                            }


                            //Is this node coeherent with the structure?
                            if (edgeMatch(queryEdge, kbEdge, newRelatedQuery)) {

                                //That's a good edge!! Add it to this related query
                                newRelatedQuery.map(queryEdge, kbEdge);

                                //Map also the concept
                                newRelatedQuery.map(queryPseudoDestination, kbPseudoDestination);


                                //log("PseudoDestination of "+ queryEdge + "is the node "+ queryPseudoDestination);

                                //The node that we are going to
                                //Does it have edges that we don't have mapped?
                                int pseudoOutgoingSize = 0;
                                Collection<Edge> pseudoOutgoingEdges = query.incomingEdgesOf(queryPseudoDestination);
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    if (newRelatedQuery.hasMapped(pseudoEdge) || pseudoEdge.equals(queryEdge)) { // && newRelatedQuery.hasMapped(pseudoEdge.getSource())) {
                                        //log(step+"Already mapped " + pseudoEdge+ " to " + newRelatedQuery.mapOf(pseudoEdge));
                                    } else { // if(newRelatedQuery.hasMapped(pseudoEdge.getSource())){
                                        pseudoOutgoingSize++;
                                    }
                                }
                                pseudoOutgoingEdges = query.outgoingEdgesOf(queryPseudoDestination);
                                for (Edge pseudoEdge : pseudoOutgoingEdges) {
                                    if (newRelatedQuery.hasMapped(pseudoEdge) || pseudoEdge.equals(queryEdge)) { // && newRelatedQuery.hasMapped(pseudoEdge.getDestination())) {
                                        //log(step+"Already mapped " + pseudoEdge + " to " + newRelatedQuery.mapOf(pseudoEdge));
                                    } else { // if(newRelatedQuery.hasMapped(pseudoEdge.getDestination())){
                                        pseudoOutgoingSize++;
                                    }
                                }

                                //Have we already visited the node that we are going to?
                                //boolean isAlreadyVisited = newRelatedQuery.hasMapped(queryPseudoDestination);




                                //Lookout! We need to check the outgoing part, if we did not already
                                if (pseudoOutgoingSize > 0) {// && !isAlreadyVisited) {
                                    // Possible outgoing branches
                                    List<RelatedQuery> tmpRelatedQueries;
                                    //Go find them!
                                    //log("Go find mapping for: " + queryPseudoDestination + " // " + kbPseudoDestination);
                                    tmpRelatedQueries = createQueries(query, queryPseudoDestination, kbPseudoDestination, newRelatedQuery, step + step);
                                    //Did we find any?
                                    if (tmpRelatedQueries != null) {
                                        //Ok so we found some, they are all good to me
                                        //More possible related queries
                                        //They already contain the root
                                        for (RelatedQuery branch : tmpRelatedQueries) {
                                            //All these related queries have found in this edge their match
                                            newRelatedQueries.add(branch);
                                        }
                                    } else {

                                        //log("This is not a valid query" + relatedQuery);
                                        //This query didn't find in this edge its match
                                        continue;
                                        //If we didn't find any, while we were supposed to, that's an error
                                        //log("we didn't find any, while we were supposed to, that's an error");
                                        //return null;
                                    }
                                } else { //if (!isAlreadyVisited) {
                                    //log("Complete query " + relatedQuery);
                                    //this related query has found in this edge is map
                                    //newRelatedQuery.map(queryPseudoDestination, kbPseudoDestination);
                                    newRelatedQueries.add(newRelatedQuery);
                                }
                            } else {
                                if (newRelatedQuery.hasMapped(queryPseudoDestination)) {
                                }
//                                if (!newRelatedQuery.mapOf(queryPseudoDestination).equals(kbPseudoDestination)) {
//                                    log(step + "And is mapped to " + newRelatedQuery.mapOf(queryPseudoDestination) + " instead of " + kbPseudoDestination);
//                                }
                            }
                        }
                    }
                }
            }
            //after this cycle we should have found some, how do we check?
            if (newRelatedQueries.isEmpty()) {
                return null;
            } else {
                //basically in the *new* list are the related queries still valid and growing
                relatedQueries = newRelatedQueries;
            }

        }


        return relatedQueries.size() > 0 ? relatedQueries : null;
    }


    protected boolean edgeMatch(Edge queryEdge, Edge kbEdge, RelatedQuery r) {

        if (queryEdge.getLabel().longValue() != kbEdge.getLabel().longValue()) {
            return false;
        }

        Long querySource = null;
        Long queryDestination = null;
        Long kbSource = null;
        Long kbDestination = null;
        boolean keep;
        Set<Long> sourceMap = null;
        Set<Long> destMap = null;

        if (r != null) {
//            if (RelatedQueryRecursive.queryGraphMap != null && RelatedQueryRecursive.queryGraphMap.size() > 0) {
//                sourceMap = RelatedQueryRecursive.queryGraphMap.get(queryEdge.getSource());
//                destMap = RelatedQueryRecursive.queryGraphMap.get(queryEdge.getDestination());
//                keep = sourceMap != null && destMap != null && sourceMap.contains(kbEdge.getSource()) && destMap.contains(kbEdge.getDestination());
//            } else {
//                keep = true;
//            }

//            if (!keep) {
//                //debug("Pruned one edge");
//                return false;
//            }

            if (r.isUsing(kbEdge)) {
                return false;
            }

            querySource = queryEdge.getSource();
            kbSource = kbEdge.getSource();

            boolean mappedSource = r.hasMapped(querySource);
            boolean usingSource = r.isUsing(kbSource);

            if (usingSource && !mappedSource) {
                return false;
            }

            queryDestination = queryEdge.getDestination();
            kbDestination = kbEdge.getDestination();

            boolean mappedDestination = r.hasMapped(queryDestination);
            boolean usingDestination = r.isUsing(kbDestination);
            if (usingDestination && !mappedDestination) {
                return false;
            }

            if (mappedSource && !r.mapOf(querySource).equals(kbSource)) {
                return false;
            }

            if (mappedDestination && !r.mapOf(queryDestination).equals(kbDestination)) {
                return false;
            }

            if(usingSource && !r.mappedAs(kbSource).equals(querySource)){
                return false;
            }

            if(usingDestination && !r.mappedAs(kbDestination).equals(queryDestination)){
                return false;
            }

        }

//        if (RelatedQuery.conceptSimilarity(querySource, kbSource) < RelatedQuery.MIN_SIMILARITY || RelatedQuery.conceptSimilarity(queryDestination, kbDestination) < RelatedQuery.MIN_SIMILARITY) {
//            return false;
//        }

        return true;
    }


    /**
     * Checks if, for a given concept, it exist an <b>outgoing</b> with that
     * label and returns all the edges found
     *
     * @param label the label we are looking for
     * @param kbEdges the knoweldgebase edges
     * @return labeled edges, can be empty
     */
    public static List<Edge> findEdges(Long label, Collection<Edge> kbEdges) {

        // Compare to the edges in the KB exiting from the mapped concept passed
        List<Edge> edges = new ArrayList<Edge>();

        for (Edge Edge : kbEdges) {
            if (label.longValue() == Edge.GENERIC_EDGE_LABEL || Edge.getLabel().longValue() == label.longValue()) {
                edges.add(Edge);
            }
        }
        return edges;
    }

}

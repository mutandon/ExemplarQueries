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
package eu.unitn.disi.db.exemplar.core.algorithms.steps;

import eu.unitn.disi.db.exemplar.core.IsomorphicQuery;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;

import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class GraphIsomorphismRecursiveStep extends AlgorithmStep<RelatedQuery> {


    private final Long queryConcept;

    public GraphIsomorphismRecursiveStep(int threadNumber, Iterator<Long> kbConcepts, Long queryConcept, Multigraph query, Multigraph targetSubgraph, boolean limitComputation, boolean skipSave) {
        super(threadNumber,kbConcepts,query, targetSubgraph, limitComputation, skipSave);
        this.queryConcept = queryConcept;
    }

    @Override
    public List<RelatedQuery> call() throws Exception {
        IsomorphicQuery relatedQuery;
        List<IsomorphicQuery> relatedQueriesPartial = new LinkedList<>();
        Set<RelatedQuery> relatedQueries = new HashSet<>();

        boolean warned = false;
        watch.start();
        while (graphNodes.hasNext()) {
            Long node = graphNodes.next();

            try {
                relatedQuery = new IsomorphicQuery(query);
                //Map the first node
                relatedQuery.map(queryConcept, node);

                relatedQueriesPartial = createQueries(query, queryConcept, node, relatedQuery);
                if (relatedQueriesPartial != null) {
                    if(skipSave){
                        continue;
                    }
                    relatedQueries.addAll(relatedQueriesPartial);
                    if (!warned && watch.getElapsedTimeMillis() > WARN_TIME && relatedQueries.size() > MAX_RELATED) {
                        warn("More than " + MAX_RELATED + " partial isomorphic results");
                        warned = true;
                        if (limitComputation) {
                            warn("Computation interrupted after " + relatedQueries.size() + " partial isomorphic results");
                            break;
                        }
                    }
                }
            } catch (OutOfMemoryError E) {
                if (relatedQueriesPartial != null) {
                    relatedQueriesPartial.clear();
                }
                error("Memory exausted, so we are returning something but not everything.");
                //System.gc();
                return new LinkedList<>(relatedQueries);
            }

            //if (watch.getElapsedTimeMillis() > WARN_TIME) {
            //    info("Computation %d [%d] took %d ms", threadNumber, Thread.currentThread().getId(), watch.getElapsedTimeMillis());
            //}
        }
        watch.stop();

        return new LinkedList<>(relatedQueries);
    }

    /**
     * Given a query, a starting node from the query, and a node from the
     * knowledgeBase , tries to build up a related query
     *
     * @param query
     * @param queryNode
     * @param graphNode
     * @return
     */
    public List<IsomorphicQuery> createQueries(Multigraph query, Long queryNode, Long graphNode, IsomorphicQuery relatedQuery) {
        // Initialize the queries set
        //Given the current situation we expect to build more than one possible related query
        List<IsomorphicQuery> relatedQueries = new ArrayList<>();
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
        queryEdgesIn = queryEdgesIn == null ? new HashSet<Edge>() : queryEdgesIn;
        queryEdgesOut = queryEdgesOut == null ? new HashSet<Edge>() : queryEdgesOut;
        graphEdgesIn = graphEdgesIn == null ? new HashSet<Edge>() : graphEdgesIn;
        graphEdgesOut = graphEdgesOut == null ? new HashSet<Edge>() : graphEdgesOut;

        //debug("TEst %d map to  %d", queryNode, graphNode);

        //Optimization: if the queryEdges are more than the kbEdges, we are done, not isomorphic!
        if (queryEdgesIn.size() > graphEdgesIn.size() || queryEdgesOut.size() > graphEdgesOut.size()) {
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

        //Look if we can map all the outgoing/ingoing graphEdges of the query node
        for (Edge queryEdge : queryEdges) {
            //info("Trying to map the edge " + queryEdge);
            List<IsomorphicQuery> newRelatedQueries = new ArrayList<>();
            LinkedList<IsomorphicQuery> toTestRelatedQueries = new LinkedList<>();

            for (IsomorphicQuery current : relatedQueries) {
                if (current.hasMapped(queryEdge)) {
                    newRelatedQueries.add(current);
                } else {
                    toTestRelatedQueries.add(current);
                }
            }

            // reset, we do not want too many duplicates
            relatedQueries = new LinkedList<>();

            // If all candidated have this QueryEdge mapped, go to next
            if(toTestRelatedQueries.isEmpty()){
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

            //loggable.debug("Matching with %d graphEdges", graphEdges.size() );
            // Do we found any?
            if (graphEdges.isEmpty()) {
                // If we cannot map graphEdges, this path is wrong
                return null;
            } else {
                //Cycle through all the possible graphEdges options,
                //they would be possibly different related queries
                for (Edge graphEdge : graphEdges) {
                    //Cycle through all the possible related queries retrieved up to now
                    //A new related query is good if it finds a match
                    for (IsomorphicQuery tempRelatedQuery : toTestRelatedQueries) {
                        if (tempRelatedQuery.isUsing(graphEdge)) {
                            //Ok this option is already using this edge,
                            //not a good choice go away
                            //it means that this query didn't found his match in this edge
                            continue;
                        }
                        //Otherwise this edge can be mapped to the query edge if all goes well
                        IsomorphicQuery newRelatedQuery = tempRelatedQuery.getClone();

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

                            //The query node that we are going to map
                            //Does it have graphEdges that we don't have mapped?
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
                                List<IsomorphicQuery> tmpRelatedQueries;
                                //Go find them!
                                //log("Go find mapping for: " + queryNextNode + " // " + graphNextNode);
                                tmpRelatedQueries = createQueries(query, queryNextNode, graphNextNode, newRelatedQuery);
                                //Did we find any?
                                if (tmpRelatedQueries != null) {
                                    //Ok so we found some, they are all good to me
                                    //More possible related queries
                                    //They already contain the root
                                    for (IsomorphicQuery branch : tmpRelatedQueries) {
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
            } else {
                //basically in the *new* list are the related queries still valid and growing
                relatedQueries = newRelatedQueries;
            }

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
    protected boolean edgeMatch(Edge queryEdge, Edge graphEdge, IsomorphicQuery r) {

        if (queryEdge.getLabel() != graphEdge.getLabel().longValue()) {
            return false;
        }

        Long querySource = null;
        Long queryDestination = null;
        Long graphSource = null;
        Long graphDestination = null;

        if (r != null) {
            if (r.isUsing(graphEdge)) {
                return false;
            }

            querySource = queryEdge.getSource();
            graphSource = graphEdge.getSource();

            boolean mappedSource = r.hasMapped(querySource);
            boolean usingSource = r.isUsing(graphSource);

            if (usingSource && !mappedSource) {
                return false;
            }

            queryDestination = queryEdge.getDestination();
            graphDestination = graphEdge.getDestination();

            boolean mappedDestination = r.hasMapped(queryDestination);
            boolean usingDestination = r.isUsing(graphDestination);
            if (usingDestination && !mappedDestination) {
                return false;
            }

            if (mappedSource && !r.isomorphicMapOf(querySource).equals(graphSource)) {
                return false;
            }

            if (mappedDestination && !r.isomorphicMapOf(queryDestination).equals(graphDestination)) {
                return false;
            }

            if (usingSource && !r.mappedAs(graphSource).equals(querySource)) {
                return false;
            }

            if (usingDestination && !r.mappedAs(graphDestination).equals(queryDestination)) {
                return false;
            }

        }
        return true;
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
        List<Edge> edges = new ArrayList<>();

        for (Edge Edge : graphEdges) {
            if (label == Edge.GENERIC_EDGE_LABEL || Edge.getLabel().longValue() == label) {
                edges.add(Edge);
            }
        }
        return edges;
    }

}

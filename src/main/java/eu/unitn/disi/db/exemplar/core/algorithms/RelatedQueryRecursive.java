/*
 * Copyright (C) 2012 Matteo Lissandrini <matteo.lissandrini at gmail.com>
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

import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.freebase.FreebaseConstants;
import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class contains a naive implementation for Algorithm1 thus the solution
 * obtained traversing the graph in order to find subgraphs matching the pattern
 * in the query given as input
 *
 * @author Matteo Lissandrini <matteo.lissandrini at gmail.com>
 */
//TODO: Use algorithm notation.
public class RelatedQueryRecursive extends RelatedQuery {
    public static Multigraph knowledgeBaseSubgraph;
    public static Map<Long,Set<Long>> queryGraphMap;
    protected static RelatedQueryRecursive loggable;
    private static final int NUMBER_OF_THREADS = 8;
    private boolean interrupt = false;
    
    public RelatedQueryRecursive(Multigraph query) {
        super(query);
    }


    @Override
    public List<RelatedQuery> findRelated(Multigraph query, Multigraph kbs, Map<Long,Set<Long>> queryGraphMap) throws DataException {
        knowledgeBaseSubgraph = kbs;
        Long queryConcept = getRootNode(query, knowledgeBaseSubgraph.edgeSet(), true);
        if (queryConcept == null) {
            loggable.error("Returning empty set as no root node has been found, and this is plain WR0NG!");
            return Collections.emptyList();
        }
        return findRelated(query, kbs, queryConcept, queryGraphMap);
    }
    
    
    public List<RelatedQuery> findRelated(Multigraph query, Multigraph kbs, Long startingNode, Map<Long,Set<Long>> queryGraphMap) throws DataException {
        RelatedQueryRecursive.loggable = new RelatedQueryRecursive(query);
        List<RelatedQuery> relatedQueries = new ArrayList<RelatedQuery>();
        StopWatch watch = new StopWatch();

        //D:ADDED BY DAVIDE: 04/30/2013
        watch.start();
        if (queryGraphMap == null) {
            queryGraphMap = new HashMap<Long, Set<Long>>();
        }
        //D:

        knowledgeBaseSubgraph = kbs;
        RelatedQueryRecursive.queryGraphMap = queryGraphMap;

        Collection<Edge> queryEdges = query.edgeSet();
        Collection<Edge> kbEdges = kbs.edgeSet();
        Multigraph restricted = new BaseMultigraph(kbs.edgeSet().size());



        Edge tmpEdge;
        Set<Long> goodNodes = new HashSet<Long>();

        Long  tmpSrc, tmpDst;
        if(queryGraphMap!=null && queryGraphMap.size() > 0){
            for (Edge queryEdge : queryEdges) {
                tmpSrc = queryEdge.getSource();
                tmpDst = queryEdge.getDestination();

                if(!queryGraphMap.containsKey(tmpSrc) || queryGraphMap.get(tmpSrc).isEmpty() ) {
                    throw new IllegalStateException("Query tables do not contain maps for the node " + FreebaseConstants.convertLongToMid(tmpSrc));
                }

                if(!queryGraphMap.containsKey(tmpDst) || queryGraphMap.get(tmpDst).isEmpty() ) {
                    throw new IllegalStateException("Query tables do not contain maps for the node " + FreebaseConstants.convertLongToMid(tmpDst));
                }


                goodNodes.addAll(queryGraphMap.get(tmpSrc));
                goodNodes.addAll(queryGraphMap.get(tmpDst));
            }
        }


        int removed = 0;
        if(queryGraphMap!=null && queryGraphMap.size() > 0){
            for (Iterator<Edge> it = kbEdges.iterator(); it.hasNext();) {
                tmpEdge = it.next();
                if(goodNodes.contains(tmpEdge.getDestination()) && goodNodes.contains(tmpEdge.getSource())) {
                    restricted.addVertex(tmpEdge.getSource());
                    restricted.addVertex(tmpEdge.getDestination());
                    restricted.addEdge(tmpEdge);
                } else {
                    removed++;
                }
            }
        } else {
            restricted = kbs;
        }


        loggable.debug("kept %d, removed %d over %d edges non mapping edges in %dms", restricted.edgeSet().size(), removed, kbEdges.size(), watch.getElapsedTimeMillis());

        knowledgeBaseSubgraph = restricted;
        goodNodes = null;

        watch.reset();

        //queryConcept = FreebaseConstants.convertMidToLong("/m/03g3w");

        loggable.debug("Root node %s ", FreebaseConstants.convertLongToMid(startingNode));
        Collection<Long> kbConcepts;
        if(queryGraphMap!=null &&queryGraphMap.size() > 0){
            kbConcepts = queryGraphMap.get(startingNode);
        } else {
            kbConcepts = knowledgeBaseSubgraph.vertexSet();
        }
        List<RelatedQuery> tmp = null;
        lists = new ArrayList<>();

        //Start in parallel
        pool = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        int chunkSize =  (int) Math.round(kbConcepts.size() / NUMBER_OF_THREADS + 0.5);

        ////////////////////// USE 1 THREAD
        //chunkSize =  kbConcepts.size();
        ////////////////////// USE 1 THREAD



        List<Long> tmpConcepts = new LinkedList<>();

        int count = 0, threadNum =0;



        for (Long concept : kbConcepts) {
            //if (conceptSimilarity(queryConcept, concept) > MIN_SIMILARITY) {
                tmpConcepts.add(concept);
                count++;

                if (count >= chunkSize) {
                    threadNum++;
                    GraphIsomorphismRecursiveStep graphI = new GraphIsomorphismRecursiveStep(threadNum, tmpConcepts.iterator(), startingNode, query, knowledgeBaseSubgraph, interrupt);

                    lists.add(pool.submit(graphI));
                    tmpConcepts = new LinkedList<Long>();
                    count = 0;
                }
            //} else {
            //     loggable.error("Similarity not satisfied..");
            //}
        }



        //Final round
        if (count != 0) {
            //debug("Chunk[%d]: from %d to %d", j, j * chunkSize, (j + 1) * chunkSize);
            GraphIsomorphismRecursiveStep graphI = new GraphIsomorphismRecursiveStep(threadNum, tmpConcepts.iterator(), startingNode, query, knowledgeBaseSubgraph, interrupt);
            lists.add(pool.submit(graphI));
            count = 0;
            threadNum++;
        }

        loggable.info("Number of Threads: %d/%d chunk size: %d Number of concepts %d", threadNum, NUMBER_OF_THREADS, chunkSize, kbConcepts.size());
        if(kbConcepts.size()==1){
            Long i = kbConcepts.iterator().next();
            loggable.debug("The lucky node is %s ", FreebaseConstants.convertLongToMid(i));
        }
        //Merge partial results
        try {
            for (int j = 0; j < lists.size(); j++) {
                tmp = lists.get(j).get();
                if (tmp!=null){
                    //debug("Graph size: %d", smallGraph.vertexSet().size());
                    relatedQueries.addAll(tmp);
                }
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(RelatedQueryRecursive.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ExecutionException ex) {
            Logger.getLogger(RelatedQueryRecursive.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            Utilities.shutdownAndAwaitTermination(pool);
        }

        watch.stop();
        loggable.info("Computed related in %dms", watch.getElapsedTimeMillis());

        return relatedQueries;
    }


    /**
     * Given a query graph it return the node that is more likely to be the root
     * of it
     *
     * @param query the query graph
     * @return the (likely-to-be) node
     */
    public static Long getRootNode(Multigraph query, Collection<Edge> kbEdges, boolean minimumFrquency) {
        Collection<Long> nodes = query.vertexSet();
        Set<Long> edgeLabels = new HashSet<Long>();
        int maxFreq = 0, tempFreq = 0;
        Long goodNode = null;


        if(kbEdges.isEmpty() ){
            throw new IllegalStateException("NO KB Edges to find a root node!");
        }

        if(query.edgeSet().isEmpty()){
            throw new IllegalStateException("NO Query Edges to find a root node!");
        }

        for (Edge l : query.edgeSet()) {
            edgeLabels.add(l.getLabel());
        }

        Long bestLabel;
        if(minimumFrquency) {
         bestLabel =findLessFrequentLabel(kbEdges, edgeLabels);
        } else {
         bestLabel =findMostFrequentLabel(kbEdges, edgeLabels);
        }

        if(bestLabel == null){
            throw new IllegalStateException("Best Label not found when looking for a root node!");
        }
        Collection<Edge> edgesIn, edgesOut;


        for (Long concept : nodes) {
            tempFreq = knowledgeBaseSubgraph.inDegreeOf(concept)+knowledgeBaseSubgraph.outDegreeOf(concept);

            edgesIn = query.incomingEdgesOf(concept);
            for (Edge Edge : edgesIn) {
                if (Edge.getLabel().equals(bestLabel)) {
                    //loggable.info("IN: Found %s with freq %d when maxFreq is %d", FreebaseConstants.convertLongToMid(concept), tempFreq, maxFreq);
                    if(tempFreq > maxFreq){
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }

            edgesOut = query.outgoingEdgesOf(concept);
            for (Edge Edge : edgesOut) {
                if (Edge.getLabel().equals(bestLabel)) {
                    //loggable.info("OUT: Found %s with freq %d  when maxFreq is %d ", FreebaseConstants.convertLongToMid(concept), tempFreq, maxFreq);
                    if(tempFreq > maxFreq){
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }
        }

        return goodNode;
    }

    public static Long findLessFrequentLabel(Collection<Edge> kbEdges, Collection<Long> allowedLabels) {
        TreeMap<Long, Integer> frequency = new TreeMap<Long, Integer>();
        int f = 0;
        int minF = kbEdges.size();
        Long bestLabel = null;

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                if (frequency.containsKey(edge.getLabel())) {
                    f = frequency.get(edge.getLabel());
                    frequency.put(edge.getLabel(), f + 1);
                } else {
                    frequency.put(edge.getLabel(), 1);
                    f = 1;
                }
            }
        }

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                f = frequency.get(edge.getLabel());
                if (f <= minF) {
                    minF = f;
                    bestLabel = edge.getLabel();
                }
            }
        }

        return bestLabel;
    }

    public static Long findMostFrequentLabel(Collection<Edge> kbEdges, Collection<Long> allowedLabels) {
        TreeMap<Long, Integer> frequency = new TreeMap<Long, Integer>();
        int f = 0;
        int maxF = 0;
        Long bestLabel = null;

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                if (frequency.containsKey(edge.getLabel())) {
                    f = frequency.get(edge.getLabel());
                    frequency.put(edge.getLabel(), f + 1);
                } else {
                    frequency.put(edge.getLabel(), 1);
                    f = 1;
                }
            }
        }

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                f = frequency.get(edge.getLabel());
                if (f > maxF) {
                    maxF = f;
                    bestLabel = edge.getLabel();
                }
            }
        }

        return bestLabel;
    }

    @Override
    public void interrupt() {
        interrupt = true;
    }


}

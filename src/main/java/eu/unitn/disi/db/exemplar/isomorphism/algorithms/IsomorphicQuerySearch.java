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
package eu.unitn.disi.db.exemplar.isomorphism.algorithms;

import eu.unitn.disi.db.exemplar.isomorphism.algorithms.steps.GraphIsomorphismRecursiveStep;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.ExemplarAnswer;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.exemplar.core.algorithms.ExemplarQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.steps.GraphSearchStep;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.ThreadUtilities;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class contains a naive - RECURSIVE - implementation for Algorithm1 thus
 * the solution obtained traversing the graph in order to find subgraphs
 * matching the pattern in the query given as input
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */
public class IsomorphicQuerySearch extends ExemplarQuerySearch<ExemplarAnswer> {

    
    /**
     * Execute the algorithm
     *
     * @throws AlgorithmExecutionException
     */
    @Override
    public void algorithm() throws AlgorithmExecutionException {
        Long startingNode = this.getRootNode(true);
       
        
        //debug("Starting node is %s",startingNode );
        if (startingNode == null) {
            throw new AlgorithmExecutionException("no root node has been found, and this is plain WR0NG!");
        }

        // FreebaseConstants.convertLongToMid(
        // debug("Root node %s ", startingNode);
        if (this.getGraph().numberOfNodes() ==0) {
            throw new AlgorithmExecutionException("NO KB Edges to find a root node!");
        }

        if (this.getQuery().numberOfNodes() ==0) {
            throw new AlgorithmExecutionException("NO Query Edges to find a root node!");
        }
        if(storeOnlyGraphs){
            exemplarGraphs = new ArrayList<>(GraphSearchStep.EXPECTED_RESULT_SIZE*4/3);
        } else {
            this.setExemplarAnswers(new ArrayList<>(GraphSearchStep.EXPECTED_RESULT_SIZE*4/3));
        }
        
        Multigraph graph = this.getGraph();
        Multigraph query = this.getQuery();

        int numGraphNodes;
        Iterator<Long> graphNodesIterator;
        

        StopWatch watch = new StopWatch();
        watch.start();
        boolean hasQueryMap = (this.getQueryToGraphMap() != null && !this.getQueryToGraphMap().isEmpty());
        boolean hasWhiteList = this.whiteList != null && !this.whiteList.isEmpty();
        
        if ( !hasQueryMap && !hasWhiteList) {
            numGraphNodes = graph.numberOfNodes();            
            graphNodesIterator = graph.iterator();
        } else if( !hasQueryMap && hasWhiteList){
            numGraphNodes = whiteList.size();
            graphNodesIterator =whiteList.iterator();                 
        } else {
            if(strictPruning && !hasWhiteList){
                whiteList = new HashSet<>();
                for ( Set<Long> goods : this.getQueryToGraphMap().values()) {
                    whiteList.addAll(goods);
                }
            }
            numGraphNodes = this.getQueryToGraphMap().getOrDefault(startingNode, Collections.<Long>emptySet()).size();
            graphNodesIterator = this.getQueryToGraphMap().getOrDefault(startingNode, Collections.<Long>emptySet()).iterator();
        }
        assert graphNodesIterator != null :  "GraphNodes to search are not allowed to be NULL";

        

        //Start in parallel
        ExecutorService pool = Executors.newFixedThreadPool(this.getNumThreads());
        int chunkSize = this.getNumThreads() == 1 ? numGraphNodes :  (int) Math.round(numGraphNodes / this.getNumThreads() + 0.5);
        List<Future<Collection<ExemplarAnswer>>> lists = new ArrayList<>();
        ////////////////////// USE 1 THREAD
        //chunkSize =  graphNodes.size();
        ////////////////////// USE 1 THREAD

        List<List<Long>> nodesChunks = new ArrayList<>((numGraphNodes+1)/chunkSize +1);
        List<Long> tmpChunk = new ArrayList<>(chunkSize+1); // NETBEANS!
        List<GraphIsomorphismRecursiveStep> isoSteps = new ArrayList<>();  
        
        int count = 0, threadNum = 0;

        
        while(graphNodesIterator.hasNext()){
            Long node = graphNodesIterator.next();
            //if (nodesSimilarity(queryConcept, node) > MIN_SIMILARITY) {
            if (count % chunkSize == 0) {
                tmpChunk = new ArrayList<>(chunkSize+1);
                nodesChunks.add(tmpChunk);
            }
            if(!strictPruning || !hasWhiteList || whiteList.contains(node)){
                tmpChunk.add(node);
                count++;
            }
        }

        for (List<Long> chunk : nodesChunks) {
            threadNum++;
            GraphIsomorphismRecursiveStep graphI = new GraphIsomorphismRecursiveStep(threadNum, chunk.iterator(), startingNode, query, graph, this.getComputationLimit(), this.getSkipSave(), whiteList, this.memoryLimit);
            isoSteps.add(graphI);
            lists.add(pool.submit(graphI));
        }
        
        //info("Number of Threads: %d/%d chunk size: %d Number of nodes %d", threadNum, this.getNumThreads(), chunkSize, graphNodes.size());
        //        if(graphNodes.size()==1){
        //            Long i = graphNodes.iterator().next();
        //            debug("The lucky node is %s ", i);
        //        }
        Collection<ExemplarAnswer> tmp;
        //Merge partial results
        try {
            for (Future<Collection<ExemplarAnswer>> list : lists) {
                try {
                    if (this.timeLimit > 0) {
                        tmp = list.get(this.timeLimit, TimeUnit.SECONDS);
                    } else {
                        tmp = list.get();
                    }
                    if (tmp != null) {
                        //debug("Graph size: %d", smallGraph.vertexSet().size());
                        //                  //((List<RelatedQuery>)this.getRelatedQueries()).addAll(tmp);
                        if(!storeOnlyGraphs){
                            List<ExemplarAnswer> rr = this.getExemplarAnswers();
                            rr.addAll(tmp);
                        } else {                        
                            for (ExemplarAnswer ans : tmp) {
                                this.exemplarGraphs.add(ans.buildMatchedGraph());                            
                            }
                            tmp.clear();
                        }
                    }
                } catch (TimeoutException | InterruptedException ex) {
                    list.cancel(true);
                    this.setInterrupted();
                    error("Killing the thread, message: %s", ex.getMessage());
                }
            }
        } catch (ExecutionException ex) {
            error("Isomorphic search failed.", ex);
        } finally {
            ThreadUtilities.shutdownAndAwaitTermination(pool);
        }
        for (GraphIsomorphismRecursiveStep isoStep : isoSteps) {
            if (isoStep.isMemoryExhausted()) {
                setMemoryExhausted(true);
                break;
            }
        }

        watch.stop();
        //debug("Computed Isomorphism in %dms", watch.getElapsedTimeMillis());
    }

    

    @Override
    public List<ExemplarAnswer> getExemplarAnswers() {
        if(this.storeOnlyGraphs){
            throw new IllegalAccessError("This algorithms only stores graph version of the answers, call getExemplarGraphs() instead");
        }
        return super.getExemplarAnswers(); 
    }

    
    
    
    
    
}

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
package eu.unitn.disi.db.exemplar.simulation.algorithms;

import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import eu.unitn.disi.db.exemplar.simulation.algorithms.steps.StrongSimulatonExpansionRecursiveStep;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.ExemplarAnswer;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.exemplar.simulation.core.SimulatedAnswer;
import eu.unitn.disi.db.exemplar.core.algorithms.ExemplarQuerySearch;
import eu.unitn.disi.db.exemplar.core.algorithms.steps.GraphSearchStep;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.ThreadUtilities;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
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
public class ConnectedSimulatedQuerySearch extends ExemplarQuerySearch<ExemplarAnswer> {

    @AlgorithmInput
    protected int maxDiameter = 0;
    
    /**
     * Execute the algorithm
     *
     * @throws AlgorithmExecutionException
     */
    @Override
    public void algorithm() throws AlgorithmExecutionException {
        Long startingNode = this.getRootNode(true);
        if (startingNode == null) {
            throw new AlgorithmExecutionException("no root node has been found, and this is plain WR0NG!");
        }

        // FreebaseConstants.convertLongToMid(
        // debug("Root node %s ", startingNode);
        if (this.getGraph().numberOfNodes()==0) {
            throw new AlgorithmExecutionException("NO KB Edges to find a root node!");
        }

        if (this.getQuery().numberOfNodes()==0) {
            throw new AlgorithmExecutionException("NO Query Edges to find a root node!");
        }

        this.setExemplarAnswers(new ArrayList<>(1000));

        Multigraph graph = this.getGraph();
        Multigraph query = this.getQuery();

        Collection<Long> graphNodes;

        StopWatch watch = new StopWatch();
        watch.start();
      
         
        graphNodes = graph.vertexSet();

        
        assert graphNodes != null :  "GraphNodes to search are not allowed to be NULL";
        
        if(this.getQueryToGraphMap() != null && strictPruning){
            whiteList = new HashSet<>();
            for ( Set<Long> goods : this.getQueryToGraphMap().values()) {
                whiteList.addAll(goods);
            }
            debug("Whitelist contains %s nodes", whiteList.size());
            graphNodes = whiteList;
        }

        if(storeOnlyGraphs){
            exemplarGraphs = new ArrayList<>(GraphSearchStep.EXPECTED_RESULT_SIZE*4/3);
        } else {
            this.setExemplarAnswers(new ArrayList<>(GraphSearchStep.EXPECTED_RESULT_SIZE*4/3));
        }
        

        //Start in parallel
        ExecutorService pool = Executors.newFixedThreadPool(this.getNumThreads());
        int chunkSize = this.getNumThreads() == 1 ? graphNodes.size() :  (int) Math.round(graphNodes.size() / this.getNumThreads() + 0.5);
        List<Future<Collection<SimulatedAnswer>>> lists = new ArrayList<>();
        List<StrongSimulatonExpansionRecursiveStep> simSteps = new ArrayList<>();  
        ////////////////////// USE 1 THREAD
        //chunkSize =  graphNodes.size();
        ////////////////////// USE 1 THREAD

        List<List<Long>> nodesChunks = new LinkedList<>();
        List<Long> tmpChunk = new LinkedList<>(); // NETBEANS!
        int count = 0, threadNum = 0;

       for (Long node : graphNodes) {
            //if (nodesSimilarity(queryConcept, node) > MIN_SIMILARITY) {
            if (count % chunkSize == 0) {
                tmpChunk = new LinkedList<>();
                nodesChunks.add(tmpChunk);

            }
            tmpChunk.add(node);
            count++;

            //} else {
            //     loggable.error("Similarity not satisfied..");
            //}
        }

        for (List<Long> chunk : nodesChunks) {
            threadNum++;
            StrongSimulatonExpansionRecursiveStep graphI = new StrongSimulatonExpansionRecursiveStep(threadNum, chunk.iterator(), query, graph, this.getComputationLimit(), this.getSkipSave(), this.whiteList, this.memoryLimit, this.maxDiameter);
            simSteps.add(graphI);
            lists.add(pool.submit(graphI));
        }

        info("Number of Threads: %d/%d chunk size: %d Number of nodes %d", threadNum, this.getNumThreads(), chunkSize, graphNodes.size());
        //        if(graphNodes.size()==1){
        //            Long i = graphNodes.iterator().next();
        //            debug("The lucky node is %s ", i);
        //        }

        //Merge partial results
        
        Collection<SimulatedAnswer> tmp;
        try {
            for (Future<Collection<SimulatedAnswer>> list : lists) {
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
                    }                } catch (TimeoutException | InterruptedException ex) {
                    list.cancel(true);
                    this.setInterrupted();
                    error("Killing the thread, message: %s", ex.getMessage());
                }
            }
              
        } catch ( ExecutionException ex) {
            error("Simulation search failed.", ex);           
        } finally {
            ThreadUtilities.shutdownAndAwaitTermination(pool);
        }

        for (StrongSimulatonExpansionRecursiveStep simStep : simSteps) {
            if (simStep.isMemoryExhausted()) {
                setMemoryExhausted(true);
                break;
            }
        }

        watch.stop();
        
        //watch.stop();
        //info("Computed %d candidate related in %dms", ans.size(),  watch.getElapsedTimeMillis());
        //boolean addAll = this.getExemplarAnswers().addAll(ans);
        //watch.reset();

    }

    public int getMaxDiam() {
        return maxDiameter;
    }

    public void setMaxDiam(int maxDiam) {
        this.maxDiameter = maxDiam;
    }

    
    
    
    
}

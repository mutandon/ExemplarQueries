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
package eu.unitn.disi.db.exemplar.core.algorithms;

import eu.unitn.disi.db.command.algorithmic.Algorithm;
import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import eu.unitn.disi.db.command.algorithmic.AlgorithmOutput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.ExemplarAnswer;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class generalizes the steps needed to search for related queries
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 * @param <T>
 */
//TODO: Use algorithm notation.
public abstract class ExemplarQuerySearch<T extends ExemplarAnswer> extends Algorithm {

    public static final double MIN_SIMILARITY = 0.5;
    public static final int DEFAULT_NUMBER_OF_THREADS = 8;

    @AlgorithmInput
    private Multigraph query;

    @AlgorithmInput
    private Multigraph graph;

    @AlgorithmInput
    private int numThreads = DEFAULT_NUMBER_OF_THREADS;

    @AlgorithmInput
    private int limitComputation = 0;

    @AlgorithmInput
    private boolean skipSave = false;

    @AlgorithmInput
    private Map<Long, Set<Long>> queryToGraphMap;

    
    @AlgorithmInput
    protected boolean strictPruning = false;

    @AlgorithmInput
    protected boolean storeOnlyGraphs = false;
    
    
    @AlgorithmOutput
    protected List<Multigraph> exemplarGraphs;
    
    @AlgorithmInput
    protected Set<Long> whiteList = null;

    
    @AlgorithmOutput
    protected List<T> answers;

    
    /**
     * traverse the portion of the graph selected in order to find subgraphs
     * matching the pattern in the query given as input
     *
     * @throws eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException
     */
    @Override
    public abstract void algorithm() throws AlgorithmExecutionException;

    
    /**
     *
     * @return  the Query we are searching for
     */
    public Multigraph getQuery() {
        return query;
    }

    /**
     *
     * @param query  to search for
     */
    public void setQuery(Multigraph query) {
        this.query = query;
    }

    /**
     *
     * @return the Graph w are searching into
     */
    public Multigraph getGraph() {
        return graph;
    }

    /**
     *
     * @param graph the graph we are searching into
     */
    public void setGraph(Multigraph graph) {
        this.graph = graph;
    }

    /**
     *
     * @return the related queries we have found
     */
    public List<T> getExemplarAnswers() {
        return answers;
    }

    /**
     *
     * @param related the related queries we have found
     */
    protected void setExemplarAnswers( List<T> related) {
        this.answers = related;
    }


    /**
     *
     * @return the parallelization level
     */
    public int getNumThreads() {
        return numThreads;
    }

    /**
     *
     * @param numThreads the parallelization level
     */
    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    /**
     *
     * @return whether we are stopping early the computation
     */
    public boolean isLimitedComputation() {
        return limitComputation > 0 ;
    }

    
    public int getComputationLimit() {
        return limitComputation ;
    }

    
    /**
     *
     * @param limitComputation se to > 0 to stop the computation early, false to compute everything
     */
    public void setLimitedComputation(int limitComputation) {
        this.limitComputation = limitComputation;
    
    }



    /**
     * Find the label which appears less often in the knowledge base
     *
     * @param allowedLabels
     * @return
     */
    public Long findLessFrequentLabel(Collection<Long> allowedLabels) {
        TreeMap<Long, Integer> frequency = new TreeMap<>();
        Collection<Edge> kbEdges = this.graph.edgeSet();
        Long bestLabel = null;
        int f;
        int minF = kbEdges.size();

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                if (frequency.containsKey(edge.getLabel())) {
                    f = frequency.get(edge.getLabel()) +1;                    
                } else {                    
                    f = 1;
                }
                frequency.put(edge.getLabel(), f);
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

    /**
     * Find the label which appears more often in the knowledge base
     *
     * @param allowedLabels
     * @return
     */
    public Long findMostFrequentLabel(Collection<Long> allowedLabels) {
        TreeMap<Long, Integer> frequency = new TreeMap<>();
        Collection<Edge> kbEdges = this.graph.edgeSet();
        Long bestLabel = null;
        int f;
        int maxF = 0;

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                if (frequency.containsKey(edge.getLabel())) {
                    f = frequency.get(edge.getLabel()) +1;                    
                } else {
                    f = 1;
                }
                frequency.put(edge.getLabel(), f);
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

    /**
     * Given a query graph it return the node that is more convenient
     * to select as root.
     * Based on the number of edges and the most frequent or infrequent label
     *
     * @param minimumFrquency
     * @return the (likely-to-be) node
     * @throws eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException
     */
    public Long getRootNode(boolean minimumFrquency) throws AlgorithmExecutionException {
        Collection<Long> nodes = this.query.vertexSet();
        //We disabled pruning based on the frequency
//        Set<Long> edgeLabels = new HashSet<>();
        int maxFreq = 0, tempFreq;
        Long goodNode = null;


//        for (Edge l : this.query.edgeSet()) {
//            edgeLabels.add(l.getLabel());
//        }

        Long bestLabel = 0L;

        for(Edge e : this.query.edgeSet()){
            bestLabel = e.getLabel() > bestLabel ? e.getLabel() : bestLabel;
        }

//        if(minimumFrquency) {
//         bestLabel =this.findLessFrequentLabel(edgeLabels);
//        } else {
//         bestLabel =this.findMostFrequentLabel(edgeLabels);
//        }

        if (bestLabel == null || bestLabel == 0L ){
            throw new AlgorithmExecutionException("Best Label not found when looking for a root node!");
        }


        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            tempFreq = this.query.inDegreeOf(concept)+ this.query.outDegreeOf(concept);

            edgesIn = this.query.incomingEdgesOf(concept);

            for (Edge Edge : edgesIn) {
                if (Edge.getLabel().equals(bestLabel)) {
                    if(tempFreq > maxFreq){
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }

            edgesOut = this.query.outgoingEdgesOf(concept);
            for (Edge Edge : edgesOut) {
                if (Edge.getLabel().equals(bestLabel)) {
                    if(tempFreq > maxFreq){
                        goodNode = concept;
                        maxFreq = tempFreq;
                    }
                }
            }
        }

        return goodNode;
    }


    /**
     * Given two concepts and their graph it compares their similarity
     *
     * @param c1 first concept to compare
     * @param c2 second concept to compare
     * @return the similarity of the two concepts in the graph
     */
    public static double conceptSimilarity(Long c1, Long c2) {
        //OF course...
        return MIN_SIMILARITY + 0.1;
    }



    /**
     *
     * @return the map for the pruning
     */
    public Map<Long, Set<Long>> getQueryToGraphMap() {
        return queryToGraphMap;
    }

    /**
     *
     * @param queryGraphMap the map to use for the pruning
     */
    public void setQueryToGraphMap(Map<Long, Set<Long>> queryGraphMap) {
        this.queryToGraphMap = queryGraphMap;
    }

    /**
     *
     * @param skipSave
     */
    public void setSkipSave(boolean skipSave) {
        this.skipSave = skipSave;
    }

    /**
     *
     * @return
     */
    public boolean getSkipSave() {
        return this.skipSave;
    }
    
    /**
     * Uses white list of nodes from queryGraphMap
     * @param strictPruning 
     */
    public void setStrictPruning(boolean strictPruning) {
        this.strictPruning = strictPruning;
    }

    public void setStoreOnlyGraphs(boolean storeOnlyGraphs) {
        this.storeOnlyGraphs = storeOnlyGraphs;
    }


    public List<Multigraph> getExemplarGraphs() {
        if(!this.storeOnlyGraphs){
            throw new IllegalAccessError("This algorithms only stores full Exemplar version of the answers, call getExemplarAnswers() instead");
        }

        return exemplarGraphs;
    }

    public Set<Long> getWhiteList() {
        return whiteList;
    }

    /**
     * If not set this is computed from the query to graph map
     * when strict pruning is enabled
     * @param whiteList 
     */
    public void setWhiteList(Set<Long> whiteList) {
        this.whiteList = whiteList;
    }
    


    
}

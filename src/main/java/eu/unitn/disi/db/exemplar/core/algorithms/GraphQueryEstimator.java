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
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.Numbers;
import eu.unitn.disi.db.mutilities.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This algorithm estimates the number of graph isomorphism matches
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */
public class GraphQueryEstimator extends Algorithm {

    @AlgorithmInput
    private Multigraph query;

    @AlgorithmInput
    private Map<Long, Integer> labelFrequency;

    @AlgorithmInput
    private Map<Pair<Long, Long>, Integer> labelPairFrequency;

    @AlgorithmInput
    private List<Map<Long,Integer>> labelDegreeFrequency;

    @AlgorithmInput
    private Map<Long, Double> totalPairFreq;
    
    @AlgorithmOutput
    private Double estimation = 0.0;
    
    @AlgorithmOutput
    private long minPNode = 0l;

    /**
     * To which extent label degrees n labelDegreeFrequency are saved so which
     * is the size of the array 
     */
    private int maxLabelDegreeFrequency = 4;

    
    
    
    /**
     * traverse the portion of the graph selected in order to find subgraphs
     * matching the pattern in the query given as input
     *
     * @throws eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException
     */
    @Override
    public void algorithm() throws AlgorithmExecutionException {

        maxLabelDegreeFrequency = this.labelDegreeFrequency.size();

        if (query.numberOfEdges() == 0) {
            throw new IllegalArgumentException("GraphQueryEstimator shouldn't be used ona single node!");
        }
        this.minPNode = query.iterator().next();
        if (query.numberOfEdges() == 1) {
            this.estimation = 1.0*Math.round(computeProbability(CollectionUtilities.getOne(query.edgeSet())));
            return;
        }
        if (query.numberOfEdges() == 2) {
            Iterator<Edge> es = query.edgeSet().iterator();
            Edge e =  es.next();
            long l1 = e.getLabel();
            this.minPNode = query.degreeOf(e.getDestination()) > 1 ? e.getDestination() : e.getSource();
            long l2 = es.next().getLabel();                                            
            this.estimation = 1.0*Math.round(computeProbability(l1, l2));
            return;
        }

                
        
        
        
        if (isStar(query)) {
            this.estimation = 1.0*Math.round(computeProbability(query.edgeSet()));
            return;
        }

        // ELSE
        // NOT A STAR!
        //        ArrayList<Pair<Long, Integer>> sortedDegrees = new ArrayList<>(query.vertexSet().size());
        //        for (Long node : query) {
        //            sortedDegrees.add(new Pair<>(node, query.inDegreeOf(node)+query.outDegreeOf(node)));
        //        }
        //        Collections.sort(sortedDegrees, new PairSecondComparator(false));        
        // Set<Long> visited = new HashSet<>();
        Double pMax = 0.0, pMin = Double.MAX_VALUE, pMul = 1.0, tmp;
        int count =0;
        for (Long n : query) {
            //Extreme nodes we don't care about
            if(query.degreeOf(n) <2){
                continue;
            }            
            tmp = computeProbability(query.edgesOf(n));
            //debug("Node: %s pp: %s ", n, tmp);
            if(tmp<pMin){
                this.minPNode = n;
                pMin = tmp;
            }            
            pMax = Math.max(tmp, pMax);
            pMul*=tmp;
            count++;
            
        }
        this.estimation = 1.0*Math.ceil(pMin);

    }

    /**
     * 
     * @param e
     * @return the probability of the current edge
     */
    public double computeProbability(Edge e) {

        return computeProbability(e.getLabel());

    }
    
    /**
     * 
     * @param l
     * @return the probability of the current edge
     */
    public double computeProbability(Long l) {

        return labelFrequency.getOrDefault(l, 0);

    }
    
    public double computeProbability(Edge e1, Edge e2){        
        return computeProbability(e1.getLabel(), e2.getLabel());
    }
    
    public double computeProbability(long l1, long l2){        
        Pair<Long, Long> keyPair = l1 < l2 ? new Pair<>(l1, l2) : new Pair<>(l2,l1);
        return computeProbability(keyPair);        
    }
  
    public double computeProbability(Pair<Long, Long> keyPair){
        if(keyPair.getFirst() > keyPair.getSecond()){
            throw new IllegalStateException("The order in the key pair is wrong "+ keyPair.getFirst() + " > " + keyPair.getSecond() );
        }
        return this.labelPairFrequency.getOrDefault(keyPair, 0);
    }
  
    
    /**
     *
     * @param q
     * @return true if the graph is a star
     */
    public boolean isStar(Multigraph q) {
        int deg = q.numberOfEdges();        
        for (Long node : q) {
            if (deg == (q.inDegreeOf(node) + q.outDegreeOf(node))) {
                boolean check = true;
                for(Edge e : q.edgeSet()){
                    check = check && (e.getDestination().equals(node) || e.getSource().equals(node));
                }
                if(check){
                    return true;
                }
                
            }
        }

        return false;
    }
    
    /**
     *
     * @param q
     * @return true if the graph is a star
     */
    public Long getCenter(Multigraph q) {
        int deg = q.numberOfEdges();
        for (Long node : q) {
            if (deg == (q.inDegreeOf(node) + q.outDegreeOf(node))) {
                return node;
            }
        }

        return null;
    }
    

    /**
     *
     * @param edgeSet
     * @return
     */
    public double computeProbability(Collection<Edge> edgeSet) throws AlgorithmExecutionException {
        List<Long> edgeLabels = new ArrayList<>(edgeSet.size());

        for (Edge edge : edgeSet) {
            edgeLabels.add(edge.getLabel());
        }

        edgeLabels = CollectionUtilities.asSortedList(edgeLabels);
        
        if (edgeLabels.size() == 1) {
            return computeProbability(edgeLabels.get(0));
        } else if( edgeLabels.size() == 2 ){            
            return computeProbability(edgeLabels.get(0), edgeLabels.get(1));
        }
        
        
        int searchDeg = Math.min(edgeSet.size(), maxLabelDegreeFrequency) -1 ;
        
        //MinDeg is the maximum number of nodes that can host matches        
        double minDeg = Double.MAX_VALUE;        
        long minLabel =0l;
        double matches, current, totalPairs, minCombinations =1;      
        
        // FIND HOW MANY JOIN NODES THERE CAN BE
        // AND HOW MAIN PAIRS  WE CAN EXTRACT
        Map<Long, Double> labelsDegF = new HashMap<>();
        for (Long eLabel : edgeLabels) {
            if(labelsDegF.containsKey(eLabel)){
                continue;
            }            
            matches = 0.0;
            totalPairs = 0.0;
            for(int d = searchDeg; d< maxLabelDegreeFrequency; d++ ){
                current = this.labelDegreeFrequency.get(d).getOrDefault(eLabel, 0);
                totalPairs += current*Numbers.binomial(d, searchDeg);
                matches+= current;                
            }
            labelsDegF.put(eLabel, matches);            
            //There are no more than those stars
            if(minDeg>matches){
                minLabel = eLabel;
                minDeg =  matches;
                minCombinations = totalPairs;
            }            
            if(matches<1){
                warn("Impossible match for label %s and degree %s", eLabel, searchDeg);
                return 0.0;
            }                                
        }
        

        
        // We have to assume that all other pairs are only partially matched 
        // in each host node
        
        double numerator = 0;
        

        double tmpPairF, pairF;
        double minLabelF = this.labelFrequency.get(minLabel);        
        double l1PairF, l2PairF;
        Double minLabelTotlF =  this.totalPairFreq.get(minLabel);
        if(minLabelTotlF == null){
            throw new AlgorithmExecutionException("Missing frequency values");
        }
        
        
        Pair<Long, Long> keyPair = new Pair<>(), l1Pair, l2Pair;        
        //debug("Min label is %s with freq %s and deg %s", minLabel, this.labelFrequency.get(minLabel), minDeg);
        for (int i = 0; i < edgeLabels.size()-1; i++) {
            long l1 = edgeLabels.get(i);
            for (int j = i+1; j < edgeLabels.size(); j++) {
                long l2 =edgeLabels.get(j);
                keyPair.setFirst(l1);
                keyPair.setSecond(l2);                        
                pairF = this.labelPairFrequency.getOrDefault(keyPair, 0);
                if(l1 == minLabel || l2 == minLabel){
                    numerator = Double.max(numerator, pairF/minLabelTotlF);
                }
                
                if(pairF < 1){
                      warn("%s FREQ FOR PAIR %s  wiht PAIR FREQ %S AND single freqs %s   %s", pairF, keyPair, this.labelPairFrequency.getOrDefault(keyPair, 0), totalPairFreq.get(l1), totalPairFreq.get(l2));
                      return 0.0;
                }                
            }
        }
        
        Double result = (minCombinations*numerator);// * scale ;        
        double maxLimit = Integer.MAX_VALUE;
        //debug("num: %s goodNodes: %s goodCombs: %s   labelFreq : %s  result: %s", numerator, minDeg, minCombinations, minLabelF, result);
        if(result > maxLimit){
            warn("!!!! \t\t Value out of bounds: %s replaced with %s", result, minDeg);
            result = minDeg;
        }
        return result;//num * den * deg;
    }

   

    /**
     *
     * @param query to search for
     */
    public void setQuery(Multigraph query) {
        this.query = query;
    }

    /**
     *
     * @return the Query we are searching for
     */
    public Multigraph getQuery() {
        return query;
    }

    /**
     *
     * @param freq
     */
    public void setLabelFrequency(Map<Long, Integer> freq) {
        this.labelFrequency = freq;
    }

    
    /**
     *
     * @param labelPairFrequency
     */
    public void setLabelPairFrequency(Map<Pair<Long, Long>, Integer> labelPairFrequency) {
        this.labelPairFrequency = labelPairFrequency;
    }

    

    /**
     *
     * @param labelDegreeFrequency
     */
    public void setLabelDegreeFrequency(List<Map<Long,Integer>> labelDegreeFrequency) {
        this.labelDegreeFrequency = labelDegreeFrequency;
    }

    /**
     * 
     * @param totalPairFreq 
     */
    public void setTotalPairFreq(Map<Long, Double> totalPairFreq) {
        this.totalPairFreq = totalPairFreq;
    }

    
    
    
    /**
     *
     * @return the estimation of the query
     */
    public Double getEstimation() {
        return estimation;
    }

    /**
     * 
     * @return  the node with minimum probability
     */
    public long getMinPNode() {
        return minPNode;
    }

    
    
    
}

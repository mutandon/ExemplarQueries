/*
 * Copyright (C) 2012 Davide Mottin <mottin@disi.unitn.eu>
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
import eu.unitn.disi.db.mutilities.data.WeightedComparator;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.comparators.EdgeLabelComparator;
import eu.unitn.disi.db.mutilities.data.CompoundIterator;
import eu.unitn.disi.db.mutilities.data.FixedSizePriorityQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Take as input the set of nodes and the set of labels in the query Finds the
 * top-setMaxNumNodes nodes with highest PPV, biased on edges and nodes present
 * in the query
 *
 *
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PersonalizedPageRank extends Algorithm {

    public static final int MIN_PARTICLES = 1;
    public static final int MAX_DEPTH = 100;
    public static final int VERTEX_INIT_CAPACITY = 500000;
    public static final int EDGES_INIT_CAPACITY = 500000;
    protected Multigraph kb;

    //INPUTS
    @AlgorithmInput
    protected double restartProbability;
    @AlgorithmInput
    protected double threshold;
    @AlgorithmInput
    protected int maxNumNodes =0;
    @AlgorithmInput
    protected boolean keepOnlyQueryEdges;
    @AlgorithmInput
    protected Map<Long, Double> labelInformativeness;
    @AlgorithmInput
    protected Collection<Long> startingNodes;
    @AlgorithmInput
    protected Collection<Long> priorityLabels;
    @AlgorithmInput
    protected Set<Long> hubs;
    @AlgorithmInput
    protected Set<Long> labelsBlacklist = null;

    @AlgorithmInput
    protected double skewFactor = 1.0;

    /**
     *  with 1.0 we will have at least 1 particle per edge in the initial nodes
     */
    @AlgorithmInput
    protected double particlePerDegree = 1.0;

    
    @AlgorithmInput
    protected boolean computeNeighborhood = true;

    //OUTPUTS
    @AlgorithmOutput
    protected Map<Long, Double> particleVector;
    @AlgorithmOutput
    protected BaseMultigraph neighborhood;
    @AlgorithmOutput
    protected int visitedNodesCount;
    @AlgorithmOutput
    protected int visitedEdgesCount;

    
    protected final double SKEW_VALUE = 1000.0;
    protected final double SHIFT_VALUE = 0.0;


    
    public PersonalizedPageRank(Multigraph kb) {
        this.kb = kb;
    }

    public void setKeepOnlyQueryEdges(boolean keep) {
        this.keepOnlyQueryEdges = keep;
    }

    public void setRestartProbability(double restartProbability) {
        this.restartProbability = restartProbability;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public void setMaxNumNodes(int k) {
        this.maxNumNodes = k;
    }

    public void setComputeNeighborhood(boolean computeNeighborhood) {
        this.computeNeighborhood = computeNeighborhood;
    }

    /**
     * Return PPR Vector with numbers normalized in the [0-1] range
     * @return map of node id to PPR value, missing nodes are zero
     */
    public Map<Long, Double> getPPRVector() {
        Map<Long, Double> pprV = new HashMap<>();
        //Normalize popularities
        Set<Long> keys = particleVector.keySet();
        double max = 0, min = Double.MAX_VALUE;
        for (Double val : particleVector.values()) {
            if (val > max) {
                max = val;
            }
            if (val < min) {
                min = val;
            }
        }
        debug("Max ppv %s  Min ppv %s", max, min);
        //Normalize
        for (Long n : keys) {
            pprV.put(n, (particleVector.get(n) - min) / (max - min));
        }
        return pprV;
    }

    public Map<Long, Double> getParticleVector() {
        return particleVector;
    }

    /**
     * Creates a clone so that we can skew it later
     *
     * @param labelFrequencies
     */
    public void setLabelInformativeness(Map<Long, Double> labelFrequencies) {
        this.labelInformativeness = labelFrequencies;
    }

    public void setStartingNodes(Collection<Long> startingNodes) {
        this.startingNodes = startingNodes;
    }
    
    public void setStartingNode( Long startingNode) {
        this.startingNodes = new HashSet<>();
        this.startingNodes.add(startingNode);
    }

    public void setPriorityLabels(Collection<Long> priorityLabels) {
        this.priorityLabels = priorityLabels;
    }

    public void setHubs(Set<Long> hubs) {
        this.hubs = hubs;
    }

    public Multigraph getNeighborhood() {
        return neighborhood;
    }

    protected void updateNeighborhoodGraph(long node) {
        Iterator<Edge> aEdges;
        boolean keepEdge;
        Edge e;
        for (int i = 0; i < 2; i++) {
            aEdges = i == 0 ? this.kb.incomingEdgesIteratorOf(node) : this.kb.outgoingEdgesIteratorOf(node);
            while (aEdges.hasNext()) {
                e = aEdges.next();
                this.visitedEdgesCount++;
                keepEdge = !this.keepOnlyQueryEdges || this.priorityLabels.contains(e.getLabel());
                if (keepEdge) {
                    this.neighborhood.addVertex(e.getSource());
                    this.neighborhood.addVertex(e.getDestination());
                    this.neighborhood.addEdge(e);
                }
            }
        }
    }

    protected void updateGraph(Multigraph graph, Iterator<Edge> edges, boolean incoming, Set<Long> allowedLabels, Set<Long> allowedNodes) {                
        if (edges == null) {
            return;
        }
        Edge edge;
        while( edges.hasNext()){
        edge = edges.next();
            this.visitedEdgesCount++;
            //edge = edge1;
            boolean keepEdge = !this.keepOnlyQueryEdges || allowedLabels.contains(edge.getLabel());
            boolean keepNode = (allowedNodes == null) || ((incoming && allowedNodes.contains(edge.getSource())) || (!incoming && allowedNodes.contains(edge.getDestination())));
            if (keepEdge && keepNode) {                
                graph.addEdge(edge);
            }
        }

    }

    protected void updateGraph(Multigraph graph, Iterator<Edge> edges, boolean incoming, Set<Long> allowedLabels) {
        updateGraph(graph, edges, incoming, allowedLabels, null);
    }

    protected void updateGraph(Multigraph graph, Iterator<Edge> edges, boolean incoming) {
        updateGraph(graph, edges, incoming, new HashSet<>(priorityLabels), null);
    }

    @Override
    protected void algorithm() throws AlgorithmExecutionException {
        //DECLARATIONS
        
        Collection<Long> graphNodes;
        HashMap<Long, Double> skewedLabelInformativeness = new HashMap<>(this.labelInformativeness);
        HashSet<Long> visitedNodes = new HashSet<>();
        Map<Long, Double> currentParticles = new HashMap<>();
        Map<Long, Double> aux;

        if(this.computeNeighborhood){
            neighborhood = new BaseMultigraph(VERTEX_INIT_CAPACITY);
        }
        particleVector = new HashMap<>(VERTEX_INIT_CAPACITY);
        if(hubs == null){
            this.hubs = new HashSet<>();
        }

        long destId;
        long cyclesCount = 0;
        double queryParticles = 0;
        double maxNodeParticles = 0;
        double partialCount = 0;

        double nodeParticles, particles, frequency, passing, currentNumParticles, normalizationWeightSum = 0.0;

        boolean notEmptyP = true;

        //STATS Vars
        StopWatch wa1 = new StopWatch();
        //double maxTime = 0;

        try {
            wa1.start();

            if (priorityLabels != null) {
                for (Long label : this.priorityLabels) {
                    skewedLabelInformativeness.merge(label, SHIFT_VALUE, (current, shift) -> {
                        return  getSkewFactor() *current * SKEW_VALUE + shift;
                    });
                }
            }

            //Assign initial particles to nodes in the query
            for (Long node : startingNodes) {
                nodeParticles = 1 / ((double) startingNodes.size() * this.threshold);
                debug("Node %s has %d neighbours and popularity %f", node, kb.degreeOf(node), nodeParticles);
                currentParticles.put(node, nodeParticles);
                particleVector.put(node, nodeParticles);
                queryParticles += nodeParticles;
                maxNodeParticles = maxNodeParticles < nodeParticles ? nodeParticles : maxNodeParticles;
            }
            debug("Query particles are %f", queryParticles);
            // Cycles until the iteration vector is empty

            while (notEmptyP) {
                cyclesCount++;
                //Accumulator
                aux = new HashMap<>();
                //Concepts from the previous iteration
                graphNodes = currentParticles.keySet();
                for (Long node : graphNodes) {
                    // We skip the big hubs, but only if they are not part of the query and not the first time
                    if (hubs.contains(node) && !startingNodes.contains(node) && visitedNodes.contains(node)) {
                        continue;
                    }
                    visitedNodes.add(node);

                    //Particles get diminished by the restart probability
                    particles = currentParticles.get(node) * (1 - restartProbability);

                    //Need to weight all the edges and normalize their weights                    
                    //maxTime = wa.getElapsedTime() < maxTime ? maxTime : wa.getElapsedTimeMillis();
                    Iterator<Edge> aEdges;
                    Edge e;
                    currentNumParticles = particles / threshold;
                    FixedSizePriorityQueue<Edge> edges;
                    Iterator<Edge> iterableEdges;
                    // Only certain edges can  be added if there are not enough
                    // particles
                    if (currentNumParticles > this.kb.degreeOf(node)) {
                        for (int i = 0; i < 2; i++) {
                            aEdges = i == 0 ? this.kb.incomingEdgesIteratorOf(node) : this.kb.outgoingEdgesIteratorOf(node);
                            while (aEdges.hasNext()) {
                                e = aEdges.next();
                                normalizationWeightSum += skewedLabelInformativeness.get(e.getLabel());
                            }
                        }

                        ArrayList<Iterator<Edge>> iters = new ArrayList<>(2);
                        iters.add(this.kb.incomingEdgesIteratorOf(node));
                        iters.add(this.kb.outgoingEdgesIteratorOf(node));
                        iterableEdges = new CompoundIterator<>(iters);
                    } else {
                        edges = new FixedSizePriorityQueue<>((int) (currentNumParticles + 1), new EdgeLabelComparator(skewedLabelInformativeness));
                        for (int i = 0; i < 2; i++) {
                            aEdges = i == 0 ? this.kb.incomingEdgesIteratorOf(node) : this.kb.outgoingEdgesIteratorOf(node);
                            while (aEdges.hasNext()) {
                                e = aEdges.next();
                                edges.add(e);
                                normalizationWeightSum += skewedLabelInformativeness.get(e.getLabel());
                            }
                        }
                        iterableEdges = edges.asList().iterator();
                    }

                    while (iterableEdges.hasNext() && particles >= threshold) {
                        e = iterableEdges.next();
                        destId = e.getSource().equals(node) ? e.getDestination() : e.getSource();
                        passing = particles * skewedLabelInformativeness.get(e.getLabel()) / normalizationWeightSum;
                        passing = passing > threshold ? passing : threshold;
                        frequency = aux.getOrDefault(destId, 0.0);
                        frequency += passing;
                        aux.put(destId, frequency);
                        particles -= passing;

                    }
                }

                currentParticles.clear();
                currentParticles = aux;

                graphNodes = currentParticles.keySet();
                partialCount = 0;
                for (Long eId : graphNodes) {
                    frequency = particleVector.getOrDefault(eId, 0.0);
                    frequency += currentParticles.get(eId);
                    partialCount += currentParticles.get(eId);
                    particleVector.put(eId, frequency);
                }
                notEmptyP = partialCount > PersonalizedPageRank.MIN_PARTICLES && cyclesCount < PersonalizedPageRank.MAX_DEPTH;
            }
            wa1.stop();

            info("Cycled %d times, ranks %s nodes and final particles remaining %f", cyclesCount, particleVector.size(), partialCount);
            info("Total time %dms", wa1.getElapsedTimeMillis());
            //info("Average time for getting edge count:   %f ms  on a mx of maxTime %f ms", ((double) binarySearchTime / cyclesEdgeCount), maxTime);
            //info("On average each binary search on both tables takes %f ms  and is requested %d ", ((double) elapsedEdge) / (cyclesEdgeCount), cyclesEdgeCount);
            //info("On average normalization requires %f ms  ", ((double) nomalizedEdgeTime) / (cyclesEdgeCount));
            wa1.reset();
            wa1.start();
            if(this.computeNeighborhood){
                Iterator<Long> iterableNodes;
                if (this.maxNumNodes > 0) {
                    FixedSizePriorityQueue<Long> sortedToAdd = new FixedSizePriorityQueue<>(this.maxNumNodes, new WeightedComparator<>(particleVector));
                    sortedToAdd.addAll(particleVector.keySet());
                    //Put nodes from the query at first place            
                    iterableNodes = sortedToAdd.iterator();
                    debug("Ranked %d nodes ", sortedToAdd.size());
                } else {
                    iterableNodes = particleVector.keySet().iterator();
                }


                //we are sure that we put at least the nodes from query
                for (Long qNode : this.startingNodes) {
                    updateNeighborhoodGraph(qNode);
                }

                // K limits the size of the neighborhood
                while (iterableNodes.hasNext() && (this.maxNumNodes == 0 || neighborhood.numberOfNodes() < this.maxNumNodes)) {
                    long cId = iterableNodes.next();
                    if (!startingNodes.contains(cId) && particleVector.get(cId) > this.threshold && !hubs.contains(cId)) {
                        this.visitedNodesCount++;
                        updateNeighborhoodGraph(cId);
                    }

                } //END FOR
                debug("neighborhood contains %d nodes and %d edges ", neighborhood.numberOfNodes(), neighborhood.numberOfEdges());
            }
            wa1.stop();
            //debug("Time here %dms", wa1.getElapsedTimeMillis());

        } catch (NullPointerException | IllegalStateException ex) {
            throw new AlgorithmExecutionException(ex);
        }

    }

    /**
     * Expand the graph by following any BFS that contains only edges in the
     * query
     *
     * @param toExpand
     * @return
     */
    public Multigraph expandWithQueryEdges(Multigraph toExpand) {
        return  expandWithQueryEdges(toExpand, Collections.<Long>emptySet());
    }

    
    /**
     * 
     * @param hubs  blacklist
     */
    public void expandWithQueryEdges(Set<Long> hubs) {
        this.expandWithQueryEdges(this.neighborhood, hubs);
    }

    public void setSkewFactor(double skewFactor) {
        this.skewFactor = skewFactor;
    }

    public double getSkewFactor() {
        return skewFactor;
    }

    public double getParticlePerDegree() {
        return particlePerDegree;
    }

    public void setParticlePerDegree(double particlePerDegree) {
        this.particlePerDegree = particlePerDegree;
    }

    public Set<Long> getLabelsBlacklist() {
        return labelsBlacklist;
    }

    public void setLabelsBlacklist(Set<Long> labelsBlacklist) {
        this.labelsBlacklist = labelsBlacklist;
    }

    
    
    
    /**
     * Expand the graph by following any BFS that contains only edges in the
     * query
     */
    public void expandWithQueryEdges() {
        if (this.neighborhood == null) {
            return;
        }
        this.expandWithQueryEdges(this.neighborhood);
    }

    public int getVisitedNodesCount() {
        return this.visitedNodesCount;
    }

    public int getVisitedEdgesCount() {
        return this.visitedEdgesCount;
    }

    /**
     * 
     * @param toExpand
     * @param hubs blacklist
     * @return 
     */
    public Multigraph expandWithQueryEdges(Multigraph toExpand, Set<Long> hubs) {
        if (toExpand == null) {
            return null;
        }
        LinkedList<Long> nodesToExpand = new LinkedList<>(toExpand.vertexSet());

        while (!nodesToExpand.isEmpty()) {
            Long node = nodesToExpand.pollFirst();
            if(!hubs.isEmpty() && hubs.contains(node)){
                continue;
            }
            Iterator<Edge> aEdges;
            Edge e;
            for (int i = 0; i < 2; i++) {
                aEdges = i == 0 ? this.kb.incomingEdgesIteratorOf(node) : this.kb.outgoingEdgesIteratorOf(node);
                while (aEdges.hasNext()) {
                    e = aEdges.next();
                    if (this.priorityLabels.contains(e.getLabel())) {
                        long candidateNode = i == 0 ? e.getSource() : e.getDestination();
                        if (!toExpand.containsVertex(candidateNode)) {
                            toExpand.addVertex(candidateNode);
                            nodesToExpand.addLast(candidateNode);
                        }
                        toExpand.addEdge(e);
                    }
                }
            }

        }
        debug("Expanded Graph contains %d nodes and %d edges", toExpand.numberOfNodes(), toExpand.numberOfEdges());

        return toExpand;

    }

    
}

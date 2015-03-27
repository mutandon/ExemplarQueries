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
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class PersonalizedPageRank extends Algorithm {

    public static final int MIN_PARTICLES = 5;
    public static final int MAX_DEPTH = 6;
    public static final int VERTEX_MAX_CAPACITY = 500000;
    public static final int EDGES_MAX_CAPACITY = 500000;
    protected BigMultigraph kb;
    //INPUTS
    @AlgorithmInput
    private double restartProbability;
    @AlgorithmInput
    private double threshold;
    @AlgorithmInput
    private int k;
    @AlgorithmInput
    private boolean keepOnlyQueryEdges;
    @AlgorithmInput
    private Map<Long, Double> labelFrequencies;
    @AlgorithmInput
    private Collection<Long> startingNodes;
    @AlgorithmInput
    private Collection<Long> priorityEdges;
    @AlgorithmInput
    private Set<Long> hubs;

    //OUTPUTS
    @AlgorithmOutput
    private Map<Long, Double> personalizedPageRank;
    @AlgorithmOutput
    private Multigraph neighborhood;
    @AlgorithmOutput
    private int visitedNodesCount;
    @AlgorithmOutput
    private int visitedEdgesCount;
    @AlgorithmOutput
    private long binarySearchTime = 0;

    public PersonalizedPageRank(BigMultigraph kb) {
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

    public void setK(int k) {
        this.k = k;
    }

    public Map<Long, Double> getPopularity() {
        return personalizedPageRank;
    }

    public void setLabelFrequencies(Map<Long, Double> labelFrequencies) {
        this.labelFrequencies = labelFrequencies;
    }

    public void setStartingNodes(Collection<Long> startingNodes) {
        this.startingNodes = startingNodes;
    }

    public void setPriorityEdges(Collection<Long> priorityEdges) {
        this.priorityEdges = priorityEdges;
    }

    public void setHubs(Set<Long> hubs) {
        this.hubs = hubs;
    }

    /**
     * Take as input the set of concepts and the set of labels in the query
     * Finds the top-k nodes with highest PPV, biased on edges and nodes present
     * in the query
     *
     * @return
     */
    public Multigraph getNeighborhood() {
        return neighborhood;
    }

    private void updateGraph(Multigraph graph, long[][] edges, boolean incoming, Set<Long> allowedLabels) {
        //long[] edge;
        long source, dest;
        if (edges == null) {
            return;
        }

        for (long[] edge : edges) {
            this.visitedEdgesCount++;
            //edge = edge1;
            if (!this.keepOnlyQueryEdges || allowedLabels.contains(edge[2])) {
                if (incoming) {
                    source = edge[1];
                    dest = edge[0];
                } else {
                    source = edge[0];
                    dest = edge[1];
                }

                graph.addVertex(source);
                graph.addVertex(dest);
                graph.addEdge(source, dest, edge[2]);

            }
        }

    }

    @Override
    public void compute() throws AlgorithmExecutionException {
        //DECLARATIONS
        neighborhood = new BaseMultigraph(VERTEX_MAX_CAPACITY);
        Collection<Long> concIDs;

        //Collection<Edge> edges;
        LinkedList<Integer> sortedIncoming;
        LinkedList<Integer> sortedOutgoing;
        LinkedList<Long> sortedNeighbors;
        HashSet<Long> visitedNodes = new HashSet<>();

        Map<Long, Double> queryLabelWeights = new HashMap<>();
        Map<Integer, Double> graphEdgeWeightsIn = new HashMap<>();
        Map<Integer, Double> graphEdgeWeightsOut = new HashMap<>();
        Map<Long, Double> p = new HashMap<>();
        personalizedPageRank = new HashMap<>();
        Map<Long, Double> aux;

        long id;
        long destId;
        long cyclesCount = 0;
        //long elapsedEdge = 0;
        long numRel = 0;
        double nomalizedEdgeTime = 0;
        double sortingEdgeTime = 0;
        double tempNomalizedEdgeTime = 0;
        double queryParticles = 0;

        double nodeParticles = 0;
        double maxNodeParticles = 0;
        double particles = 0;
        double partialCount = 0;
        double frequency = 0;
        double passing = 0;
        double averageEdgeWeight = 0;
        boolean notEmptyP = true;

        //STATS Vars
        StopWatch wa = new StopWatch();
        StopWatch wa1 = new StopWatch();
        double maxTime = 0;

        try {
            wa1.start();

            // Get label prior for Query Edges
            // IS Used!!
            if (priorityEdges != null) {
                for (Long edge : priorityEdges) {
                    queryLabelWeights.put(edge, labelFrequencies.get(edge));
                }
            }
            //DEBUG:


            //Assign initial particles to nodes in the query
            for (Long conc : startingNodes) {
                id = conc;
                nodeParticles = (1 / (double) startingNodes.size()) / (this.threshold);
                numRel = kb.inDegreeOf(conc) + kb.outDegreeOf(conc);
                debug("Node %s has %d neighbours and popularity %f", id, numRel, nodeParticles);
                p.put(id, nodeParticles);
                personalizedPageRank.put(id, nodeParticles);
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
                concIDs = p.keySet();
                //visitedConc = 0;
                //debug("Looping over %d concepts", concIDs.size());
                for (Long eId : concIDs) {
                    // We skip the big hubs, but only if they are not part of the query and not the first time
                    if (hubs.contains(eId) && ( !startingNodes.contains(eId)) || visitedNodes.contains(eId)) {
                        //debug("skipping the topic");
                        continue;
                    }
                    visitedNodes.add(eId);


                    //Particles get diminished by the restart probability
                    particles = p.get(eId) * (1 - restartProbability);

                    //Need to weight all the edges and normalize their weights
                    wa.reset();
                    wa.start();
                    numRel = kb.inDegreeOf(eId) + kb.outDegreeOf(eId);
                    wa.stop();
                    binarySearchTime += wa.getElapsedTimeMillis();
                    maxTime = wa.getElapsedTime() < maxTime ? maxTime : wa.getElapsedTimeMillis();

                    wa.start();
                    long[][] incoming = kb.incomingArrayEdgesOf(eId);
                    long[][] outgoing = kb.outgoingArrayEdgesOf(eId);
                    wa.stop();

                    wa.reset();

                    //Obtain edge weights
                    wa.start();
                    graphEdgeWeightsIn.clear();
                    graphEdgeWeightsOut.clear();
                    //INCOMING
                    partialCount = 0;
                    averageEdgeWeight = 2 * particles / threshold;
                    if (incoming != null) {
                        for (int i = incoming.length - 1; i > -1; i--) {
                            if (queryLabelWeights.containsKey(incoming[i][2])) {
                                frequency = queryLabelWeights.get(incoming[i][2]);
                            } else {
                                frequency = 0;
                            }
                            frequency += labelFrequencies.get(incoming[i][2]);
                            graphEdgeWeightsIn.put(i, frequency);
                            partialCount += frequency;
                            if (incoming.length - i > averageEdgeWeight) {
                                break;
                            }
                        }
                    }
                    //OUTGOING
                    if (outgoing != null) {
                        for (int i = outgoing.length - 1; i > -1; i--) {
                            if (queryLabelWeights.containsKey(outgoing[i][2])) {
                                frequency = queryLabelWeights.get(outgoing[i][2]);
                            } else {
                                frequency = 0;
                            }
                            frequency += labelFrequencies.get(outgoing[i][2]);
                            graphEdgeWeightsOut.put(i, frequency);
                            partialCount += frequency;
                            if (outgoing.length - i > averageEdgeWeight) {
                                break;
                            }
                        }
                    }

                    //Normalization step
                    if (incoming != null) {
                        for (int i = incoming.length - 1; i > -1; i--) {
                            if (!graphEdgeWeightsIn.containsKey((i))) {
                                break;
                            }
                            graphEdgeWeightsIn.put(i, graphEdgeWeightsIn.get(i) / partialCount);
                        }
                    }
                    if (outgoing != null) {
                        for (int i = outgoing.length - 1; i > -1; i--) {
                            if (!graphEdgeWeightsOut.containsKey((i))) {
                                break;
                            }
                            graphEdgeWeightsOut.put(i, graphEdgeWeightsOut.get(i) / partialCount);
                        }
                    }
                    wa.stop();
                    tempNomalizedEdgeTime = wa.getElapsedTimeMillis();
                    wa.reset();
                    wa.start();
                    // Sorting outcome to give priority to more important edges
                    if (incoming != null) {
                        sortedIncoming = new LinkedList(graphEdgeWeightsIn.keySet());
                        Collections.sort(sortedIncoming, new WeightedComparator(graphEdgeWeightsIn));
                    } else {
                        sortedIncoming = new LinkedList<>();
                    }

                    if (outgoing != null) {
                        sortedOutgoing = new LinkedList(graphEdgeWeightsOut.keySet());
                        Collections.sort(sortedOutgoing, new WeightedComparator(graphEdgeWeightsOut));
                    } else {
                        sortedOutgoing = new LinkedList<>();
                    }

                    wa.stop();
                    sortingEdgeTime = wa.getElapsedTimeMillis();
                    nomalizedEdgeTime += tempNomalizedEdgeTime;
                    wa.reset();

                    Integer firstIn = null, firstOut = null;
                    wa.start();
                    while (particles > threshold) {
                        firstIn = sortedIncoming.peekLast();
                        firstOut = sortedOutgoing.peekLast();
                        if (firstIn == null && firstOut == null) {
                            break;
                        }
                        if (firstOut == null || (firstIn != null && graphEdgeWeightsIn.get(firstIn) > graphEdgeWeightsOut.get(firstOut))) {
                            frequency = graphEdgeWeightsIn.get(firstIn);
                            long[] edge = incoming[firstIn];
                            destId = edge[1];
                            passing = particles * frequency;
                            passing = passing > threshold ? passing : threshold;
                            sortedIncoming.pollLast();
                        } else {
                            frequency = graphEdgeWeightsOut.get(firstOut);
                            long[] edge = outgoing[firstOut];
                            destId = edge[1];
                            passing = particles * frequency;
                            passing = passing > threshold ? passing : threshold;
                            sortedOutgoing.pollLast();
                        }
                        frequency = 0;
                        if (aux.containsKey(destId)) {
                            frequency = aux.get(destId);
                        }
                        frequency += passing;
                        aux.put(destId, frequency);
                        particles = particles - passing;
                    }
                    wa.stop();
                    if (wa.getElapsedTimeMillis() > 100) {
                        // debug("%d edges over % d - Cycles %d - NormTime %d + SortTime %d + DistTime %d millis", edgesConsidered, numRel, cyclesEdgeCount, (long) tempNomalizedEdgeTime, (long) sortingEdgeTime, (long) wa.getElapsedTimeMillis());
                    }
                }
                //debug("In this iteration %d over %d concepts were already visited", visitedConc, concIDs.size());

                p.clear();
                p.putAll(aux);

                concIDs = p.keySet();
                partialCount = 0;
                for (Long eId : concIDs) {
                    frequency = 0;
                    if (personalizedPageRank.containsKey(eId)) {
                        frequency = personalizedPageRank.get(eId);
                    }
                    frequency += p.get(eId);
                    partialCount += p.get(eId);
                    personalizedPageRank.put(eId, frequency);
                }
                notEmptyP = partialCount > PersonalizedPageRank.MIN_PARTICLES && cyclesCount < PersonalizedPageRank.MAX_DEPTH;

                //debug("Total weight in vector %f", partialCount);
                //debug("Cycle %d | Iternal cycles %d | Currently %d values | time for this %d", cyclesCount, cyclesEdgeCount, p.keySet().size(), wa1.getElapsedTimeMillis());
            }
            wa1.stop();

            info("Cycled %d times final particles remaining %f", cyclesCount, partialCount);
            info("Total time %dms", wa1.getElapsedTimeMillis());
            //info("Average time for getting edge count:   %f ms  on a mx of maxTime %f ms", ((double) binarySearchTime / cyclesEdgeCount), maxTime);
            //info("On average each binary search on both tables takes %f ms  and is requested %d ", ((double) elapsedEdge) / (cyclesEdgeCount), cyclesEdgeCount);
            //info("On average normalization requires %f ms  ", ((double) nomalizedEdgeTime) / (cyclesEdgeCount));
            sortedNeighbors = new LinkedList(personalizedPageRank.keySet());
            Collections.sort(sortedNeighbors, new WeightedComparator(personalizedPageRank, true));
            debug("Ranked %d nodes ", sortedNeighbors.size());
            wa1.reset();
            wa1.start();

            //Put nodes from the query at first place
            sortedNeighbors.removeAll(startingNodes);

            for (Long eId : startingNodes){
                sortedNeighbors.addFirst(eId);
            }

            for (Long eId : sortedNeighbors) {
                this.visitedNodesCount++;
                if (personalizedPageRank.get(eId) > this.threshold && (startingNodes.contains(eId) ||  !hubs.contains(eId))) {
                    long[][] incoming = kb.incomingArrayEdgesOf(eId);
                    long[][] outgoing = kb.outgoingArrayEdgesOf(eId);
                    updateGraph(neighborhood, incoming, true, queryLabelWeights.keySet());
                    updateGraph(neighborhood, outgoing, false, queryLabelWeights.keySet());
                } else {
                    //  debug("Skipping " + FreebaseConstants.convertLongToMid(eId));
                }
                //Break only if we are sure that we put at least the nodes from query
                // K limits the size of the neighborhood
                if(this.k > 0 && this.visitedNodesCount > this.startingNodes.size() && neighborhood.numberOfNodes() > this.k ){
                    break;
                }
            } //END FOR

            wa1.stop();
            //debug("Time here %dms", wa1.getElapsedTimeMillis());

        } catch (NullPointerException | IllegalStateException ex) {
            throw new AlgorithmExecutionException(ex);
        }

//        if(!graph.edgeSet().containsAll(query.edgeSet())){
//            for (Edge e : query.edgeSet()) {
//                if(!graph.edgeSet().contains(e)){
//                    try {
//                        error("MISSING " + FreebaseConstants.convertLongToMid(e.getSource())+ " " + FreebaseConstants.getPropertyMid(e.getLabel()) + " " + FreebaseConstants.convertLongToMid(e.getDestination()));
//                        error("MISSING " + e.getSource()+ " " +e.getLabel()+ " " + e.getDestination());
//                    } catch (IOException ex) {
//                        Logger.getLogger(PersonalizedPageRank.class.getName()).log(Level.SEVERE, null, ex);
//                    }
//                }
//            }
//            throw new IllegalStateException("Neighborhood does not contain the query ");
//        } else {
//            debug("CHECK OK : Neighborhood at least contains the query edges");
//        }
    }

    public int getVisitedNodesCount() {
        return this.visitedNodesCount;
    }

    public int getVisitedEdgesCount() {
        return this.visitedEdgesCount;
    }

    public long getBinarySearchTime() {
        return this.binarySearchTime;
    }

    private class WeightedComparator<T> implements Comparator<T> {

        private Map<T, Double> weights;
        private boolean inverse;

        public WeightedComparator(Map<T, Double> weights) {
            this.weights = weights;
            this.inverse = false;
        }

        public WeightedComparator(Map<T, Double> weights, boolean inverse) {
            this.weights = weights;
            this.inverse = inverse;
        }

        @Override
        public int compare(T i1, T i2) {
            if (inverse) {
                return -1 * (weights.get(i1).compareTo(weights.get(i2)));
            }
            return weights.get(i1).compareTo(weights.get(i2));
        }
    }
}

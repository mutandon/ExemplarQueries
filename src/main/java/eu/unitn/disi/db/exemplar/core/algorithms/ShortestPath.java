/*
 * Copyright (C) 2014 Davide Mottin <mottin@disi.unitn.eu>
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
import eu.unitn.disi.db.exemplar.core.WeightedPath;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class ShortestPath extends Algorithm {   
    @AlgorithmInput
    private Collection<Long> sources; 
    @AlgorithmInput
    private Multigraph graph; 
    
    @AlgorithmOutput
    private List<WeightedPath> shortestPaths; 
    
    private static final int ITERATIONS = 1000000;
    
    
    protected class NodeDistances {
        private final Set<Long> visitedNodes;
        //private final TreeMap<KeyPair,Double> minDistNodes; 
        
        private final Map<Long,Double> minDistances; 
        private final PriorityQueue<Object[]> q; 
        
        //private final Long source; 
        private final Map<Long,Edge> previousEdge; 
        
        public NodeDistances(Long source) {
//            this.source = source; 
            visitedNodes = new HashSet<>();
            minDistances = new HashMap<>(); 
            q = new PriorityQueue<>(10, new Comparator<Object[]>() {
                @Override
                public int compare(Object[] o1, Object[] o2) {
                    double w1 = (Double)o1[1], w2 = (Double)o2[1];
                    if (w1 > w2) return 1; 
                    if (w1 < w2) return -1; 
                    return 0; 
                }
            }); 
            minDistances.put(source, 0.0);
            q.add(new Object[]{source, 0.0});
            previousEdge = new HashMap<>();
        }
    }    
    
    
    @Override
    public void compute() throws AlgorithmExecutionException {
        final NodeDistances[] nodeDistances = new NodeDistances[sources.size()]; 
        NodeDistances dist2; 
        shortestPaths = new ArrayList<>(); 
        Object[] minDistNode;
        Pair<Integer, Integer> pathPair; 
        Map<Long, Set<Integer>> commonVisitedNodes = new HashMap<>(); 
        Set<Integer> visitedSources; 
        Set<Pair<Integer,Integer>> pathPairs = new HashSet<>();
        WeightedPath path; 
        Long node; 
        int i, j; 
        int count = 0; 
        boolean bigMultigraph = graph instanceof BigMultigraph;
        //1. Initialize
        i = 0; 
        for (Long src : sources) {
            nodeDistances[i] = new NodeDistances(src);
            i++;
        }
        //1.1 Build the path pairs to be created (all the possible path pairs) 
        for (i = 0; i <  sources.size(); i++) {
            for (j = i + 1; j < sources.size(); j++) {
                pathPairs.add(new Pair<>(i,j));
            }
        }
        //Maybe add the number of visited edges for statistical purpose
        while (!pathPairs.isEmpty() && count < ITERATIONS) {
            count++;
            //2. Single Step in each source -- PARALLELIZABLE!!!
            for (i = 0; i < nodeDistances.length; i++) {
                NodeDistances dist =  nodeDistances[i];
                //System.out.printf("Trying to extract from %d ", dist.source);
                minDistNode = dist.q.poll();
                //System.out.printf("Extracting node %d from node %d - distance: %f\n", minDistNode.node, dist.source, minDistNode.weight);
                node = (Long)minDistNode[0];
                
                if (bigMultigraph) {
                    updateDistances(((BigMultigraph)graph).incomingArrayEdgesOf(node), node, dist, minDistNode, true);
                    updateDistances(((BigMultigraph)graph).outgoingArrayEdgesOf(node), node, dist, minDistNode, false);
                } else {
                    updateDistances(graph.incomingEdgesOf(node), dist, minDistNode, true);
                    updateDistances(graph.outgoingEdgesOf(node), dist, minDistNode, false);
                }
                
                visitedSources = commonVisitedNodes.get(node);
                //3. If visited intersect, store the path - increment the counter
                if (visitedSources == null) {
                    visitedSources = new HashSet<>();
                } else {
                    for (Integer source : visitedSources) {
                        int nodeA, nodeB; 
                        if (i < source) {
                            nodeA = i; 
                            nodeB = source;
                        } else {
                            nodeA = source; 
                            nodeB = i; 
                        }
                        pathPair = new Pair<>(nodeA, nodeB);
                        //Check if we have already computed the path. 
                        if (pathPairs.contains(pathPair)) {
//                            System.out.printf("Removed path pair %s\n", pathPair);
                            pathPairs.remove(pathPair);
                            //Build the path.
                            path = buildPartialPath(node, dist.previousEdge, dist.minDistances, true);
                            dist2 = nodeDistances[source];
                            path.merge(buildPartialPath(node, dist2.previousEdge, dist2.minDistances, false));
                            shortestPaths.add(path);
                        } 
                    }
                }
                visitedSources.add(i);
                commonVisitedNodes.put(node, visitedSources);
                
                dist.visitedNodes.add(node);
            }
        }
        if (count >= ITERATIONS) {
            warn("Shortest path stopped because the number of iterations (%d) has exceeed", ITERATIONS);
        }
    }

           
    protected double distance(Long label, Long adjNode) {
        return 1.0; 
    }
    
    private void updateDistances(Collection<Edge> neighborEdges, NodeDistances dist, Object[] minDistNode, boolean incoming) {
        Long adjNode;
        Double oldWeight, newWeight; 
        Object[] newDist; 
        if (neighborEdges != null) {
            for (Edge e : neighborEdges) {
                adjNode = incoming? e.getSource(): e.getDestination();
                if (!dist.visitedNodes.contains(adjNode)) {
                    oldWeight = dist.minDistances.get(adjNode);
                    //if weight is null then the distance has not been computed yet
                    //edgeWeight = edgeWeights.get(e.getLabel());
                    //edgeWeight = edgeWeight == null? 0.0 : edgeWeight;
                    newWeight = (Double)minDistNode[1] + distance(e.getLabel(), adjNode);
                    if (oldWeight == null || newWeight < oldWeight) {
                        newDist = new Object[]{adjNode, newWeight};
                        //Update weights
                        //System.out.printf("Added/updated %d node to %d - new distance: %f - old distance: %f\n", adjNode, dist.source, minDistNode.getWeight() + edgeWeight, oldWeight);
                        dist.q.remove(newDist);
                        dist.q.add(newDist);
                        dist.minDistances.put(adjNode, newWeight);
                        dist.previousEdge.put(adjNode, e);
                    }
                }
            }
        }
    }
    
    private void updateDistances(long[][] neighborEdges, long node, NodeDistances dist, Object[] minDistNode, boolean incoming) {
        long adjNode;
        Double oldWeight, newWeight; 
        Object[] newDist; 
        if (neighborEdges != null) {
            for (long[] e : neighborEdges) {
                adjNode = e[1];
                if (!dist.visitedNodes.contains(adjNode)) {
                    oldWeight = dist.minDistances.get(adjNode);
                    //if weight is null then the distance has not been computed yet
                    //edgeWeight = edgeWeights.get(e.getLabel());
                    //edgeWeight = edgeWeight == null? 0.0 : edgeWeight;
                    newWeight = (Double)minDistNode[1] + distance(e[2], adjNode);
                    if (oldWeight == null || newWeight < oldWeight) {
                        newDist = new Object[]{adjNode, newWeight};
                        //Update weights
                        //System.out.printf("Added/updated %d node to %d - new distance: %f - old distance: %f\n", adjNode, dist.source, minDistNode.getWeight() + edgeWeight, oldWeight);
                        dist.q.remove(newDist);
                        dist.q.add(newDist);
                        dist.minDistances.put(adjNode, newWeight);
                        dist.previousEdge.put(adjNode, incoming? new Edge(adjNode, node, e[2]) : new Edge(node, adjNode, e[2]));
                    }
                }
            }
        }
    }
    
    private WeightedPath buildPartialPath(Long startingNode, Map<Long,Edge> previousEdge, Map<Long,Double> minDistances, boolean reverse) {
        //System.out.println("Called");
        List<Edge> edges  = new ArrayList<>();
        double weight;  
        Edge prev = previousEdge.get(startingNode);
        weight = minDistances.get(startingNode);
        while (prev != null) {
            edges.add(prev);
            prev = previousEdge.get(prev.getSource() == startingNode.longValue()? prev.getDestination() : prev.getSource());
            if (prev != null) {
                startingNode = startingNode.longValue() == prev.getSource()? prev.getDestination() : prev.getSource();
            }
        }
        if (reverse) {
            Collections.reverse(edges);
        }
        return new WeightedPath(weight, edges); 
    }

    public void setSources(Collection<Long> sources) {
        this.sources = sources;
    }

    public void setGraph(Multigraph graph) {
        this.graph = graph;
    }

    public List<WeightedPath> getShortestPaths() {
        return shortestPaths;
    }        
}

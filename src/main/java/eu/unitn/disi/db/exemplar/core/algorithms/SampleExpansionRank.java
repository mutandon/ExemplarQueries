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

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.mutilities.data.WeightedComparator;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.mutilities.data.CompoundIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Take as input the set of nodes and the set of labels in the query Finds the
 * top-setMaxNumNodes nodes with highest PPV, biased on edges and nodes present
 * in the query
 *
 *
 *
 * @author Matteo Lissandri <ml@disi.unitn.eu>
 */
public class SampleExpansionRank extends PersonalizedPageRank {

    public SampleExpansionRank(Multigraph kb) {
        super(kb);
    }

    protected final double SKEW_VALUE = 2.0;
    protected final double SHIFT_VALUE = 0.0;
    public static final int MAX_DEPTH = 10;

    @Override
    protected void algorithm() throws AlgorithmExecutionException {
        //DECLARATIONS

        Collection<Long> graphNodes;
        HashSet<Long> visitedNodes = new HashSet<>(VERTEX_INIT_CAPACITY);
        Map<Long, Double> currentParticles = new HashMap<>();
        Map<Long, Double> aux;

        if (this.computeNeighborhood) {
            neighborhood = new BaseMultigraph(VERTEX_INIT_CAPACITY);
        }
        particleVector = new HashMap<>(VERTEX_INIT_CAPACITY);
        if (hubs == null) {
            this.hubs = new HashSet<>();
        }

        if(this.priorityLabels == null){
            this.priorityLabels = new HashSet<>();
        }
        long destId;
        long cyclesCount = 0;
        double queryParticles = 0;
        double maxNodeParticles = 0;
        double partialCount = 0;

        double nodeParticles, particles, frequency, passing;

        boolean notEmptyP = true;

        //STATS Vars
        StopWatch wa1 = new StopWatch();
        //double maxTime = 0;
        List<Edge> toAdd = new ArrayList<>();
        if (this.computeNeighborhood) {
            toAdd = new ArrayList<>(EDGES_INIT_CAPACITY);
        }
        try {
            wa1.start();

            //Assign initial particles to nodes in the query
            for (Long node : startingNodes) {
                
                nodeParticles = Math.min(200_000.0, this.kb.degreeOf(node) * this.particlePerDegree / (this.threshold));
                if(queryParticles > Integer.MAX_VALUE){
                    error("Particle values are out!");
                    throw new AlgorithmExecutionException("Invalid Parameters Particle per Degree %s Threshold %s", this.particlePerDegree , this.threshold);
                }
                
                //debug("Node %s has %d neighbours and popularity %f", node, kb.degreeOf(node), nodeParticles);
                currentParticles.put(node, nodeParticles);
                particleVector.put(node, nodeParticles);
                queryParticles += nodeParticles;
                maxNodeParticles = maxNodeParticles < nodeParticles ? nodeParticles : maxNodeParticles;
            }
            
            debug("Query particles are %f with %s nodes", queryParticles, startingNodes.size());
            // Cycles until the iteration vector is empty

            while (notEmptyP) {
                cyclesCount++;
                //Accumulator
                aux = new HashMap<>(currentParticles.size()*5/3);
                //Concepts from the previous iteration
                graphNodes = currentParticles.keySet();
                for (Long node : graphNodes) {
                    // We skip the big hubs, but only if they are not part of the query and not the first time
                    if (visitedNodes.contains(node)) {
                        continue;
                    }
                    

                    //Particles get diminished by the restart probability later
                    particles = currentParticles.get(node);
                    int deg = this.kb.degreeOf(node);
                    // High degree nodes are sinks
                    if (particles < deg/2) {
                        continue;
                    }
                    visitedNodes.add(node);
                    //Need to weight all the edges and normalize their weights                    
                    //maxTime = wa.getElapsedTime() < maxTime ? maxTime : wa.getElapsedTimeMillis();                    
                    Edge e;
                    Iterator<Edge> iterableEdges;
                    // Only certain edges can  be added if there are not enough
                    // particles

                    ArrayList<Iterator<Edge>> iters = new ArrayList<>(2);
                    iters.add(this.kb.incomingEdgesIteratorOf(node));
                    iters.add(this.kb.outgoingEdgesIteratorOf(node));
                    iterableEdges = new CompoundIterator<>(iters);
                    double damping;
                    while (iterableEdges.hasNext()) {
                        e = iterableEdges.next();
                        destId = e.getSource().equals(node) ? e.getDestination() : e.getSource();
                        if (this.computeNeighborhood) {
                            boolean keepEdge = !this.keepOnlyQueryEdges || this.priorityLabels.contains(e.getLabel());
                            if (keepEdge) {
                                toAdd.add(e);
                            }
                        }                        
                        if (particleVector.containsKey(destId)) {
                            continue;
                        }
                        damping = this.priorityLabels.contains(e.getLabel()) ? 1 : (1 - restartProbability);
                        passing = particles * damping / (deg);
                        if (passing >= 1 || this.priorityLabels.contains(e.getLabel())) {
                            passing = Math.max(passing, 1);
                            aux.merge(destId, passing, Double::sum);
                        }                        
                    }
                }

                currentParticles.clear();
                currentParticles = aux;

                graphNodes = currentParticles.keySet();
                partialCount = 0;
                double tmp;
                for (Long eId : graphNodes) {
                    frequency = particleVector.getOrDefault(eId, 0.0);
                    tmp = currentParticles.get(eId);
                    frequency += tmp;
                    partialCount += tmp;
                    particleVector.put(eId, frequency);
                }
                notEmptyP = partialCount > MIN_PARTICLES && cyclesCount < MAX_DEPTH;

                if (this.timeLimit > 0 && this.timeLimit < wa1.getElapsedTimeMillis()) {
                    this.setInterrupted();
                    debug("Time limit");
                    notEmptyP = false;
                }
            }
            wa1.stop();

            info("Cycled %d times, ranks %s nodes and final particles remaining %f", cyclesCount, particleVector.size(), partialCount);
            info("Total time %dms", wa1.getElapsedTimeMillis());
            //info("Average time for getting edge count:   %f ms  on a mx of maxTime %f ms", ((double) binarySearchTime / cyclesEdgeCount), maxTime);
            //info("On average each binary search on both tables takes %f ms  and is requested %d ", ((double) elapsedEdge) / (cyclesEdgeCount), cyclesEdgeCount);
            //info("On average normalization requires %f ms  ", ((double) nomalizedEdgeTime) / (cyclesEdgeCount));

            if (this.computeNeighborhood) {
                wa1.reset();
                wa1.start();
                for (Edge e : toAdd) {
                    this.neighborhood.forceAddEdge(e);
                }
                wa1.stop();
                debug("neighborhood contains %d nodes and %d edgesin %d ms ", neighborhood.numberOfNodes(), neighborhood.numberOfEdges(), wa1.getElapsedTimeMillis());
            }

            //debug("Time here %dms", wa1.getElapsedTimeMillis());
        } catch (NullPointerException | IllegalStateException ex) {
            throw new AlgorithmExecutionException(ex);
        }

    }

}

/*
 * Copyright (C) 2013 Davide Mottin <mottin@disi.unitn.eu>
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
import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import eu.unitn.disi.db.grava.vectorization.NodeVectors;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;

/**
 * Compute node vectors (vectorization of the neighborhood in terms of labels)
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class NodeVectorization extends Algorithm {

    @AlgorithmInput
    private int maxNeighbors;
    @AlgorithmInput
    private int k;
    @AlgorithmInput
    private BigMultigraph graph;
    @AlgorithmInput
    private File frequentNodes;
    @AlgorithmInput
    private File indexDirectory;
    @AlgorithmInput
    private File labelFrequency;
    @AlgorithmInput
    private double minLabelProbability;
    @AlgorithmInput
    private long lastNode = -1; 
    @AlgorithmInput
    private List<Long> graphNodes; 
    
    @AlgorithmOutput
    private NodeVectors vectors;
    
    
    public NodeVectorization() {
    }

    @Override
    public void compute() throws AlgorithmExecutionException {
        Set<Long> nextLevelToSee;
        long label, nodeToAdd;
        int count = 0;
        short in;
        int numNodes, nodeEdges, edgeCount, inLen, outLen;
        Integer countNeighbors;
        Set<Long> visited;
        Set<Long> toVisit;
        //Long node;
        Map<Long, Integer> levelTable, actualTable;
        long[][] inOutEdges, inEdges, outEdges;
        
        //Collection<Long> graphNodes = graph.vertexSet();
        BufferedReader reader = null;
        String[] splittedLine;
        Set<Long> frequent = new HashSet<Long>();
        Set<Long> frequentToInclude = new HashSet<Long>();
        Set<Long> keys;
        String line;
        Map<Long, Map<Long, Integer>[]> frequentVectors;
        Map<Long, Integer>[] nodeVector, frequentVector;
        Map<Long, Double> edgeProbability;
        StopWatch time = new StopWatch();
        Iterator<Long> graphIterator;
        Long node;
        String indexFile = null, vectorFile;

        try {
            if (!indexDirectory.exists()) {
                indexDirectory.mkdir();
            }
            indexFile = indexDirectory.getAbsolutePath() + File.separator + "vectors.index";
            vectorFile = indexDirectory.getAbsolutePath() + File.separator + "vectors.dat";
            vectors = new NodeVectors(indexFile, vectorFile, labelFrequency, graph.numberOfNodes(), k, NodeVectors.Mode.WRITE);
            info("Started vectorization on graph with %d nodes", graph.numberOfNodes());
            frequentVectors = new HashMap<Long, Map<Long, Integer>[]>();

            reader = new BufferedReader(new FileReader(frequentNodes));
            while ((line = reader.readLine()) != null) {
                splittedLine = Utilities.fastSplit(line, ' ', 2);
                frequent.add(Long.parseLong(splittedLine[0]));
            }


            time.start();
            for (Long n : frequent) {
                toVisit = new HashSet<Long>();
                toVisit.add(n);
                visited = new HashSet<Long>();
                nodeVector = new Map[k - 1];

                //We can parallelize
                for (short l = 0; l < k - 1; l++) {
                    numNodes = 0; //max stored nodes per level
                    levelTable = new HashMap<Long, Integer>();
                    nextLevelToSee = new HashSet<Long>();

                    for (Long current : toVisit) {
                        //current = toVisit.poll();
                        if (current == null) {
                            warn("Current is null for level %d and node %d", n, l, current);
                        }
                        for (in = 0; in < 2; in++) { //Cycles over incoming and outgoing
                            inOutEdges = in == 0 ? graph.incomingArrayEdgesOf(current) : graph.outgoingArrayEdgesOf(current);
                            if (inOutEdges != null) {
                                for (int j = 0; j < inOutEdges.length; j++) {
                                    //for (Edge edge : inOutEdges) {
                                    if (numNodes++ > maxNeighbors) {
                                        break;
                                    }
                                    label = inOutEdges[j][2];
                                    nodeToAdd = inOutEdges[j][1];
                                    if (!visited.contains(nodeToAdd) && !frequent.contains(nodeToAdd)) {
                                        countNeighbors = levelTable.get(label);
                                        if (countNeighbors == null) {
                                            countNeighbors = 0;
                                        }
                                        levelTable.put(label, countNeighbors + 1);
                                        //Add the if it is not in the same level 
                                        if (!toVisit.contains(nodeToAdd)) {
                                            nextLevelToSee.add(nodeToAdd);
                                        }
                                    }
                                }
                            }
                        } //END FOR                       
                        visited.add(current);
                    } //END FOR
                    toVisit = nextLevelToSee;
                    nodeVector[l] = levelTable;
                    //currentIndexFuture = indexPool.submit(new UpdateIndex(levelTable, node, i));
                    //vectors.addNodeVector(levelTable, node, l);
                }
                frequentVectors.put(n, nodeVector);
                //END FOR
            }
            debug("Computed frequent node vectors in %dms", time.getElapsedTimeMillis());

            count = 0;
            time.reset();
            
            
            if (graphNodes != null) {
                graphIterator = graphNodes.iterator();
            } else { 
                graphIterator = graph.iterator();
            }
            
            
            if (lastNode != -1) {
                while(graphIterator.hasNext()) {
                    node = graphIterator.next();
                    if (node.longValue() == lastNode) {
                        break;
                    }
                }
            }
            //debug("Processing %d nodes", graph.numberOfNodes());
            while (graphIterator.hasNext()) {
                node = graphIterator.next();
                if (!frequent.contains(node)) {
                    //node = graphNodes[i];
                    toVisit = new HashSet<Long>();
                    toVisit.add(node);
                    visited = new HashSet<Long>();
                    nodeVector = new Map[k];
                    for (int i = 0; i < nodeVector.length; i++) {
                        nodeVector[i] = new HashMap<Long, Integer>();
                    }
                    //We can parallelize
                    for (short l = 0; l < k; l++) {
                        levelTable = nodeVector[l];
                        nextLevelToSee = new HashSet<Long>();
                        for (Long current : toVisit) {
                            
                            //current = toVisit.poll();
                            if (current == null) {
                                warn("NodeToExplore is null for level %d and node %d", l, node);
                            }
                            inEdges = graph.incomingArrayEdgesOf(current);
                            outEdges = graph.outgoingArrayEdgesOf(current);
                            inLen = inEdges == null?  0 : inEdges.length;
                            outLen = outEdges == null?  0 : outEdges.length;
                            nodeEdges = inLen + outLen;
                            edgeProbability = new HashMap<Long, Double>();
                            computeEdgeProbability(edgeProbability, inEdges, nodeEdges);
                            computeEdgeProbability(edgeProbability, outEdges, nodeEdges);
                            
                            for (in = 0; in < 2; in++) { //Cycles over incoming and outgoing
                                inOutEdges = in == 0 ? inEdges : outEdges;
                                if (inOutEdges != null) {
                                    for (int j = 0; j < inOutEdges.length; j++) {
                                        //for (Edge edge : inOutEdges) {
                                        label = inOutEdges[j][2];
                                        nodeToAdd = inOutEdges[j][1];
                                        if (!visited.contains(nodeToAdd)) {
                                            if (!frequent.contains(nodeToAdd)) {
                                                countNeighbors = levelTable.get(label);
                                                if (countNeighbors == null) {
                                                    countNeighbors = 0;
                                                }
                                                levelTable.put(label, countNeighbors + 1);
                                                //Add the node if it is not in the same level 
                                                if (!toVisit.contains(nodeToAdd)) {
                                                    nextLevelToSee.add(nodeToAdd);
                                                }
                                            //add the frequent only if it helps discriminating the node
                                            } else if (edgeProbability.get(label) > minLabelProbability) { 
                                                frequentVector = frequentVectors.get(nodeToAdd);
                                                //Copy the frequent vector in the actual vector
                                                for (int i = 0; (i + l) < k && i < frequentVector.length; i++) {
                                                    actualTable = frequentVector[i];
                                                    keys = actualTable.keySet();
                                                    for (Long key : keys) {
                                                        edgeCount = actualTable.get(key);
                                                        countNeighbors = nodeVector[i + l].get(label);
                                                        if (countNeighbors == null) {
                                                            countNeighbors = 0;
                                                        }
                                                        nodeVector[i + l].put(label, countNeighbors + edgeCount);
                                                    }
                                                }
                                                visited.add(nodeToAdd);
                                            } 
                                        }
                                    }
                                }
                            } //END FOR                       
                            visited.add(current);
                        } //END FOR
                        toVisit = nextLevelToSee;
                        //currentIndexFuture = indexPool.submit(new UpdateIndex(levelTable, node, i));
                        //nodeVector[l].putAll(levelTable);
                    } //END FOR
                    count++;

                    vectors.addNode(nodeVector, node);
                    // STORE INTO THE FILE(s) AFTER A NUMBER of ITERATION(s) such 
                    // that we minimize the overload and the number of disk accesses
                    // We do it concurrently so we can go on with the computation
                    // If we have less than let's say [x]Mb free memory flush the tables 
                    // into file(s)
//                if (MemoryUtils.getFreeMemory() < 100 * 1024 * 1024) { 
//                    System.gc();
//                }

                    //debug("Processed %d nodes", count);
                    if (count % 100000 == 0) {
                        debug("Processed and serialized %d/%d nodes in %dms, where the last one is %d", count, graph.numberOfNodes(), time.getElapsedTimeMillis(), node);
                        vectors.serialize();
                        time.reset();

                        //System.gc();
                    }
                }//END IF IS FREQUENT
                else {
                    frequentToInclude.add(node);
                }

            } // END FOR
            //vectors.serialize();
            
            for (Long n : frequentToInclude) {
                vectors.addNode(frequentVectors.get(n), n);
                count++;
            }
            vectors.serialize();
            debug("Processed %d nodes", count);
        } catch (IOException ex) {
            log(Level.ERROR, ex, "");
            throw new AlgorithmExecutionException("Cannot create the files to store the tables", ex);
        } catch (DataException ex) {
            log(Level.ERROR, ex, "");
            throw new AlgorithmExecutionException("Some error occurred in the serialization of the %d node", ex, count);
        } finally {
            Utilities.close(vectors);
            Utilities.close(reader);
//            Comparator<String> comparator = new Comparator<String>() {
//                @Override
//                public int compare(String r1, String r2){
//                        return r1.compareTo(r2);
//                }
//            };
//            try {
//                File iFile = new File(indexFile);
//                List<File> l = ExternalSort.sortInBatch(new File(indexFile), comparator);
//                iFile.delete();
//                ExternalSort.mergeSortedFiles(l, new File(indexDirectory.getAbsolutePath() + File.separator + "vectors.index"), comparator);
//            } catch (Exception ex) {
//                log(Level.FATAL, ex, "Cannot sort index file %s", indexFile);
//                throw new AlgorithmExecutionException("Cannot sort index file");
//            }
        }
    }
    
    private static void computeEdgeProbability(Map<Long, Double> edgeFrequency, long[][] edges, int nodeEdges) {
        Double freq; 
        if (edges != null) {
            for (int i = 0; i < edges.length; i++) {
                freq = edgeFrequency.get(edges[i][2]);
                if (freq == null) {
                    freq = 0.0;
                }
                edgeFrequency.put(edges[i][2], (freq * nodeEdges + 1)/nodeEdges);
            }
        }
    }
    
    
    public void setK(int k) {
        this.k = k;
    }

    public void setGraph(BigMultigraph graph) {
        this.graph = graph;
    }

    public void setFrequentNodes(File frequentNodes) {
        this.frequentNodes = frequentNodes;
    }

    public void setIndexDirectory(File indexDirectory) {
        this.indexDirectory = indexDirectory;
    }

    public void setLabelFrequency(File labelFrequency) {
        this.labelFrequency = labelFrequency;
    }

    public void setMaxNeighbors(int maxNeighbors) {
        this.maxNeighbors = maxNeighbors;
    }

    public double getMinLabelProbability() {
        return minLabelProbability;
    }

    public void setMinLabelProbability(double minLabelProbability) {
        this.minLabelProbability = minLabelProbability;
    }

    public void setStartingNode(long startingNode) {
        this.lastNode = startingNode;
    }

    public void setGraphNodes(List<Long> graphNodes) {
        this.graphNodes = graphNodes;
    }
    
    
    public NodeVectors getVectors() {
        return vectors;
    }
}

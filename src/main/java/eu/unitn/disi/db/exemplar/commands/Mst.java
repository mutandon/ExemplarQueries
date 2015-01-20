/*
 * The MIT License
 *
 * Copyright 2014 Davide Mottin <mottin@disi.unitn.eu>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package eu.unitn.disi.db.exemplar.commands;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.DynamicInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.WeightedPath;
import eu.unitn.disi.db.exemplar.core.algorithms.MostInformativePath;
import eu.unitn.disi.db.exemplar.core.algorithms.PersonalizedPageRank;
import eu.unitn.disi.db.exemplar.freebase.FreebaseConstants;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Pair;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class Mst extends Command {
    private BigMultigraph graph;
    private double threshold;
    private double restartProb;
    private int neighborSize;
    private String[] startingNodes;
    private String labelFrequenciesFile;
    private String hubsFile;

    private static class Entity {

        private String mid;
        private String name;
        private double relevance;

        public Entity(String mid, double relevance) {
            this.mid = mid;
            this.relevance = relevance;
        }

        public String getMid() {
            return mid;
        }

        public void setMid(String mid) {
            this.mid = mid;
        }

        public double getRelevance() {
            return relevance;
        }

        public void setRelevance(double relevance) {
            this.relevance = relevance;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Entity{" + "mid=" + mid + '}';
        }
    }

    private static class Triple {

        private String source;
        private String destination;
        private String edge;

        public Triple() {
        }

        public Triple(String source, String destination, String edge) {
            this.source = source;
            this.destination = destination;
            this.edge = edge;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public String getEdge() {
            return edge;
        }

        public void setEdge(String edge) {
            this.edge = edge;
        }

        @Override
        public String toString() {
            return "Edge{" + "source=" + source + ", destination=" + destination + ", edge=" + edge + '}';
        }
    }

    @Override
    protected void execute() throws ExecutionException {
//        List<Entity> entities = new ArrayList<>();
        StopWatch watch = new StopWatch();
        PersonalizedPageRank ppv;
        Multigraph neighborhood;
        MostInformativePath shortAlgo;
        Map<Long,Double> labelFrequencies = new HashMap<>();
        double frequencies = 0.0;
        double labelPrior = 0;
        List<Long> sources;
        Set<Long> hubs = new HashSet<>();
        List<WeightedPath> paths;

//        entities.add(new Entity("/m/055t58", 1399.337158));
//        entities.add(new Entity("/m/09jcvs", 276.4039));
//        entities.add(new Entity("/m/0qcrj", 655.279785));
//        entities.add(new Entity("/m/09c7w0", 84.771828));

        System.out.printf("Input entities %s\n",Arrays.toString(startingNodes));
        //TODO: generalize not only for Freebase
        //What if we put particles differently using the scores???
        watch.start();
        try {
            Utilities.readFileIntoMap(labelFrequenciesFile, " ", labelFrequencies, Long.class, Double.class);
            info("Read %s label file", labelFrequenciesFile);
            Utilities.readFileIntoCollection(hubsFile, hubs, Long.class);
            info("Read %s hubs file", hubsFile);

            sources = new ArrayList<>();
//            for (Entity en : entities) {
//                sources.add(FreebaseConstants.convertMidToLong(en.mid));
//            }
            for (String s : startingNodes) {
                sources.add(FreebaseConstants.convertMidToLong(s));
            }
            //Inverse document frequency (the higher the better)
            for (double freq : labelFrequencies.values()) {
                frequencies += freq;
            }
            Map<Long,Double> edgeWeights = new HashMap<>(labelFrequencies);
            Set<Long> labels = labelFrequencies.keySet();
            for (Long label : labels) {
                labelPrior = -Math.log(labelFrequencies.get(label) / (frequencies));
                labelFrequencies.put(label, labelPrior);
            }
            watch.reset();
            ppv = new PersonalizedPageRank(graph);
            //ppv.setQuery(queryGraph);
            ppv.setThreshold(threshold);
            ppv.setRestartProbability(restartProb);
            ppv.setK(neighborSize);
            ppv.setLabelFrequencies(labelFrequencies);
            ppv.setStartingNodes(sources);
            ppv.setHubs(hubs);
            ppv.setKeepOnlyQueryEdges(false);
            ppv.compute();

            neighborhood = ppv.getNeighborhood();
            info("Computed relevant neighborhood in %dms", watch.getElapsedTimeMillis());
            info("Relevant neighborhood has %d nodes and %d edges", neighborhood.vertexSet().size(), neighborhood.edgeSet().size());

//            Map<Long, Double> invertedEdgeWeights = new HashMap<>();
//            for (Long label: labelFrequencies.keySet()) {
//                invertedEdgeWeights.put(label, 1/labelFrequencies.get(label));
//            }

            watch.reset();
            shortAlgo = new MostInformativePath();
            shortAlgo.setAlpha(1.2);
            shortAlgo.setBeta(1.2);
            shortAlgo.setStepPenalty(0.5);
            shortAlgo.setNodePopularities(ppv.getPopularity());
            shortAlgo.setGraph(neighborhood);
            shortAlgo.setEdgeWeights(edgeWeights);
            shortAlgo.setNormalize(true);
            shortAlgo.setSources(sources);
            shortAlgo.compute();
            paths = shortAlgo.getShortestPaths();
            //paths = ShortestPath.shortestPath(neighborhood, sources, invertedEdgeWeights);
            info("Computed shortest paths in %dms", watch.getElapsedTimeMillis());
            //Declaration block for MST to be put into another method
            //List<Pair<Double,List<Edge>>> weightedPaths = new ArrayList<>();
            Collection<Edge> topPath;
            List<Pair<Long,Long>> endPoints = new ArrayList<>();
            Set<Long> consideredEndPoints;
            Pair<Long,Long> endPoint;
            List<Triple> mst;
            double weight;

            //Compute weights
//            for (WeightedPath path : paths) {
//                info(path.toString());
//                weight = 0;
//                for (Edge e : path.getPath()) {
//                    weight += 1/invertedEdgeWeights.get(e.getLabel());
//                }
//                weightedPaths.add(new Pair<>(weight,path));
//            }

            //Sort the paths by decreasing weights
            Collections.sort(paths);
            //Collections.reverse(paths);

            //Compute endpoints
            for (WeightedPath wPath: paths) {
                topPath = wPath.getPath();
                consideredEndPoints = new HashSet<>();
                for (Edge e : topPath) {
                    if (consideredEndPoints.contains(e.getSource())) {
                        consideredEndPoints.remove(e.getSource());
                    } else {
                        consideredEndPoints.add(e.getSource());
                    }
                    if (consideredEndPoints.contains(e.getDestination())) {
                        consideredEndPoints.remove(e.getDestination());
                    } else {
                        consideredEndPoints.add(e.getDestination());
                    }
                }
                assert consideredEndPoints.size() == 2;
                if (consideredEndPoints.size() != 2 ) {
                    error("The number of endpoints for path %s is != 2", topPath.toString());
                } else {
                    Iterator<Long> it = consideredEndPoints.iterator();
                    endPoints.add(new Pair<>(it.next(), it.next()));
                }
            }
            info("Computed path endpoints: %s", endPoints);

            //Compute MST using Kruskal algorithm
            info("Computing MST with Kruskal algorithm");
            watch.reset();
            consideredEndPoints = new HashSet<>();
            mst = new ArrayList<>();
            for (int i = 0; i < paths.size(); i++) {
                topPath = paths.get(i).getPath();
                //Check if we create a cycle.
                endPoint = endPoints.get(i);
                if (!consideredEndPoints.contains(endPoint.getFirst()) || !consideredEndPoints.contains(endPoint.getSecond())) {
                    consideredEndPoints.add(endPoint.getFirst());
                    consideredEndPoints.add(endPoint.getSecond());
                    for (Edge rel : topPath) {
                        mst.add(
                            new Triple(FreebaseConstants.convertLongToMid(rel.getSource()),
                                    FreebaseConstants.convertLongToMid(rel.getDestination()),
                                    FreebaseConstants.convertLongToMid(rel.getLabel())
                            ));
                    }
                }
            }
            info("Computed MST (Minimum Spanning Tree) in %dms", watch.getElapsedTimeMillis());
            info("MST: %s", mst);
        } catch (ParseException ex) {
            log(Level.FATAL, ex, "Parse error in the input file");
        } catch (NullPointerException | IOException ex) {
            log(Level.FATAL, ex, null);
        } catch (Exception ex) {
            log(Level.FATAL, ex, "Some other problem occurred");
        }




//        try (org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
//            watch.start();
//            for (int i = 0; i < entities.size(); i++) {
//                for (int j = i + 1; j < entities.size(); j++) {
//                    result = engine.execute(String.format(QUERY_PATH_FORMAT, FreebaseConstants.convertMidToLong(entities.get(i).getMid()), FreebaseConstants.convertMidToLong(entities.get(j).getMid())));
//                    paths = result.columnAs(PATH_VARIABLE);
//                    topPaths = new ArrayList<>();
//                    while (paths.hasNext()) {
//                        p = paths.next();
//                        System.out.println("found path " + p);
//                        topPaths.add(p);
//                    }
//                    // Database operations go here
////                System.out.println(result.dumpToString());
//                    orderedPaths.add(new Pair<>(p.length(), topPaths));
//                }
//            }//            System.out.printf("Computed all paths in %dms\n", watch.getElapsedTimeMillis());
//
//            Collections.sort(orderedPaths, new PairFirstComparator(false));
            //MST code
            //ADD some kind of ranking.
//            Set<Long> consideredEndPoints = new HashSet<>();
//            List<Edge> graph = new ArrayList<>();
//            Node source, dest;

//            watch.reset();
//
//            }
//            System.out.printf("Computed mst in %dms\n", watch.getElapsedTimeMillis());
//            System.out.println(graph);
//            tx.success();
//        } finally {
//             gds.shutdown();
//        }

    }

    @Override
    protected String commandDescription() {
        return "Compute a minimum spanning tree given a set of nodes";
    }

    @DynamicInput(
        consoleFormat = "--graph",
        description = "multigraph used as a knowledge-base")
    public void setGraph(BigMultigraph graph) {
        this.graph = graph;
    }

    @CommandInput(
        consoleFormat = "-t",
        defaultValue = "1",
        mandatory = false,
        description = "pruning threshold"
    )
    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    @CommandInput(
        consoleFormat = "-c",
        defaultValue = "0.15",
        mandatory = false,
        description = "restart probability"
    )
    public void setRestartProb(double restartProb) {
        this.restartProb = restartProb;
    }

    @CommandInput(
        consoleFormat = "-s",
        defaultValue = "5000",
        mandatory = false,
        description = "neighbor size"
    )
    public void setNeighborSize(int neighborSize) {
        this.neighborSize = neighborSize;
    }

    @CommandInput(
        consoleFormat = "--labels",
        defaultValue = "",
        mandatory = true,
        description = "label frequency file formatted as 'labelid frequency'"
    )
    public void setLabelFrequenciesFile(String labelFrequenciesFile) {
        this.labelFrequenciesFile = labelFrequenciesFile;
    }

    @CommandInput(
        consoleFormat = "--hubs",
        defaultValue = "",
        mandatory = true,
        description = "the big hubs file"
    )
    public void setHubsFile(String hubsFile) {
        this.hubsFile = hubsFile;
    }

    @CommandInput(
        consoleFormat = "--sources",
        defaultValue = "",
        mandatory = true,
        description = "the sources"
    )
    public void setStartingNodes(String[] startingNodes) {
        this.startingNodes = startingNodes;
    }
}

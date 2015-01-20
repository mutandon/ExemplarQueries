

package eu.unitn.disi.db.exemplar.commands;

import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import eu.unitn.disi.db.exemplar.core.WeightedPath;
import eu.unitn.disi.db.exemplar.core.algorithms.WeightedShortestPath;
import eu.unitn.disi.db.exemplar.freebase.FreebaseConstants;
import eu.unitn.disi.db.grava.exceptions.ParseException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class ComputeShortestPath extends Command {
    public static <T extends Multigraph> HashMap<Long, String> loadGraph(String filename, T graph) throws ParseException, NullPointerException  {
        Map<String, Long> edgeLabelsCache = new HashMap<>();
        HashMap<Long, String> midLabels = new HashMap<>();

        try {
            File file = new File(filename);
            BufferedReader in = new BufferedReader(new FileReader(file));
            long sourceId;
            long destId;
            long edgeLabel,lastAddedLabel=0;
            String line, source, dest, label;
            String[] tokens;
            int count = 0;
            if (graph == null) {
                throw new NullPointerException("Input graph cannot be null");
            }

            while ((line = in.readLine()) != null) {
                count++;
                if (!"".equals(line.trim()) && !line.trim().startsWith("#")) { //Comment
                    //System.out.println(line);
                    tokens = Utilities.fastSplit(line, ' ', 3); // split on whitespace
                    if (tokens.length < 3) { // line too short
                        tokens = Utilities.fastSplit(line, '\t', 3);
                        if (tokens.length != 3) {
                            throw new ParseException("Line " + count +" is malformed");
                        }
                    }
                    //TODO: This is not the proper way to handle this
                    source = tokens[0];
                    dest = tokens[1];
                    label = tokens[2];

                    try {
                        sourceId = Long.parseLong(source);
                        destId = Long.parseLong(dest);
                        edgeLabel = Long.parseLong(label);
                    } catch (NumberFormatException nfex) {
                        sourceId = FreebaseConstants.convertMidToLong(source);
                        destId = FreebaseConstants.convertMidToLong(dest);
                        edgeLabel = Edge.GENERIC_EDGE_LABEL;
                        if (label != null) {
                            label = label.trim();
                            if (label.equals("isA")) {
                                edgeLabel = FreebaseConstants.ISA_ID;
                            } else if(edgeLabelsCache.containsKey(label)){
                                edgeLabel= edgeLabelsCache.get(label);
                            } else {
//                                LinkedList<String> edgeLabelName = new LinkedList<String>();
//                                edgeLabelName.add(label);
                                try{
                                    edgeLabel = FreebaseConstants.getPropertyId(label);
                                } catch(NullPointerException ex){
                                    //logger.error("Cannot parse "+label);
                                    throw ex;
                                }
                                if( edgeLabel >= 0 ) {
                                    edgeLabelsCache.put(label, edgeLabel);
                                }  else {
                                    lastAddedLabel++;
                                    edgeLabelsCache.put(label, lastAddedLabel);
                                    edgeLabel = lastAddedLabel;
                                }
                            }

                            midLabels.put(edgeLabel, label);
                        }
                    }




//                    if (!concepts.containsKey(sourceId)) {
//                        //sourceNode = new Concept(sourceName, (nodeIndex++) + "");
//                        sourceNode = new Concept(sourceId);
//                        concepts.put(sourceId, sourceNode);
                    graph.addVertex(sourceId);
//                    } else {
//                        sourceNode = concepts.get(sourceId);
//                    }

//                    if (!concepts.containsKey(destId)) {
//                        //destinationNode = new Concept(destinationName, (nodeIndex++) + "");
//                        destinationNode = new Concept(destId);
//                        concepts.put(destId, destinationNode);
                    graph.addVertex(destId);
//                    } else {
//                        destinationNode = concepts.get(destId);
//                    }
                    graph.addEdge(sourceId, destId, edgeLabel);
                }
                if (count % 100000 == 0) {
                    //logger.debug("Processed %d lines of file %s", count, filename);
                }
            }

//            Collection<Long> mappedConcepts = graph.vertexSet();
//            String logString = "";
//            for (Long concept : mappedConcepts) {
//                logString += concept.toString() + " | ";
//            }
//            logger.debug(logString + "\n");
//
//            Collection<Edge> mappedEdges = graph.edgeSet();
//            logString = "";
//            for (Edge edge : mappedEdges) {
//                logString += edge.getSource() + "-[" + edge.getLabel() + "]->" + edge.getDestination() + " ; ";
//            }
//            logger.debug(logString + "\n ");
        } catch (IOException ex) {
            //Logger.getLogger(GraphFilesManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return midLabels;
    }

     public static boolean exportGraphToNET(String filename, Multigraph g, String metadata, boolean onlyISA)  {
        boolean result = false;
        TreeMap<Long, Integer> idMap = new TreeMap<Long, Integer>();



        if (g == null) {
            //throw new LoadException("Graph cannot be null");
        }

        if (onlyISA) {
            filename += ".isA";
        }
        filename += ".net";


        BufferedWriter bw = null;
        Collection<Long> verteces = g.vertexSet();
        Collection<Edge> outEdges = g.edgeSet();

        long minVertex = -1;
        int counter = 0;
        boolean addVertex = false;
        for (Long c : verteces) {
            addVertex = false;
            //If we export only isA,
            //then we will add just nodes that are connected through them
            if (onlyISA) {
                Collection<Edge> edges = g.incomingEdgesOf(c);
                edges.addAll(g.outgoingEdgesOf(c));
                for (Edge edge : edges) {
                    if (edge.getLabel().equals(FreebaseConstants.ISA_ID)) {
                        addVertex = true;
                        break;
                    }
                }
            }

            if (!onlyISA || addVertex) {
                counter++;
                idMap.put(c, counter);
                if (minVertex == -1L || minVertex > c) {
                    minVertex = c;
                }
            }
        }
        minVertex--;

        try {
            bw = new BufferedWriter(new FileWriter(filename));

            //Append metadata
            if (metadata != null) {
                bw.append("%META:" + metadata).append("\n");
            }

            //Append minimum node ID, structure, and start Vertices list
            bw.append("%").append(minVertex + "").append("\n")
                    .append("%*Colnames \"URL_MID\"")
                    .append("\n\n*Vertices\n");

            for (Long c : verteces) {
                if (!onlyISA || idMap.containsKey(c)) {
                    bw.append(idMap.get(c) + " " + c + "\n");
                }
            }

            bw.append("\n*Arcs\n");

            for (Edge edge : outEdges) {
                if (!onlyISA || idMap.containsKey(edge.getSource())) {
                    bw.append(idMap.get(edge.getSource()) + " ")
                            .append(idMap.get(edge.getDestination()) + " ")
                            .append((edge.getLabel() == FreebaseConstants.ISA_ID ? "1" : "10") + "\n");
                }
            }

            result = true;
        } catch (IOException ex) {
            //throw new LoadException("Cannot write file %s", filename, ex);
        } finally {
            Utilities.close(bw);
        }

        return result;
    }



    @Override
    protected void execute() throws ExecutionException {
        BaseMultigraph graph = new BaseMultigraph();
        try {
            loadGraph("InputData/test.graph", graph);

            info("Graph has %d nodes and %d edges", graph.vertexSet().size(), graph.edgeSet().size());
            WeightedShortestPath shortAlgo = new WeightedShortestPath();


            List<Long> startingNodes = new ArrayList<>();
            startingNodes.add(0L);
            startingNodes.add(3L);

            Map<Long, Double> edgeLabels = new HashMap<>();
            for (long i = 0; i < 20; i++) {
                edgeLabels.put(i, (double)i);
            }

            shortAlgo.setSources(startingNodes);
            shortAlgo.setEdgeWeights(edgeLabels);
            shortAlgo.setGraph(graph);

            shortAlgo.compute();
            List<WeightedPath> paths = shortAlgo.getShortestPaths();
            for (WeightedPath path : paths) {
                System.out.println(path);
            }
            exportGraphToNET("prova", graph, "", false);
        } catch (ParseException ex) {
            Logger.getLogger(ComputeShortestPath.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(ComputeShortestPath.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected String commandDescription() {
        return "Shortest path between multiple nodes";
    }

}

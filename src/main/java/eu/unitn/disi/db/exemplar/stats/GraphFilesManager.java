/*
 * Copyright (C) 2012 Matteo Lissandrini <matteo.lissandrini at gmail.com>
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
package eu.unitn.disi.db.exemplar.stats;

import eu.unitn.disi.db.command.util.LoggableObject;
import eu.unitn.disi.db.exemplar.exceptions.LoadException;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Load and saves query graphs into disk
 *
 * @author Matteo Lissandrini <matteo.lissandrini at gmail.com>
 */
public final class GraphFilesManager extends LoggableObject {
    public static final int VIZ_MAX_EXPORTABLE_DIMENSION  = 200;

    private GraphFilesManager() {
    }

    //public static final long DEFAULT_EDGE = Edge.GENERIC_EDGE;
    public static boolean saveGraph(String filename, Multigraph g) throws LoadException {
        return saveGraph(filename, g, null, null);
    }

    /**
     * Write the input graph into file. Header comments and metadata can be
     * specified.
     *
     * @param filename The file where to write the graph
     * @param g The input graph
     * @param metadata The metadata to be used (they appear as #META: into the
     * file). Put null to leave the row blank
     * @param comments The header comments to the file: put null to skip the
     * metadata
     * @return true if the process succeeds, false otherwise
     * @throws LoadException If an error occurs in the import process
     */
    public static boolean saveGraph(String filename, Multigraph g, String metadata, List<String> comments)
            throws LoadException {
        boolean result = false;
        if (g == null) {
            throw new LoadException("Graph cannot be null");
        }
        BufferedWriter bw = null;
        Collection<Long> verteces;
        Collection<Edge> outEdges;

        try {
            bw = new BufferedWriter(new FileWriter(filename));
            //BFS
            if (metadata != null) {
                bw.append("#META:" + metadata).append("\n");
            }
            if (comments != null) {
                for (String comment : comments) {
                    bw.append("#").append(comment).append("\n");
                }
            }
            verteces = g.vertexSet();
            for (Long c : verteces) {
                outEdges = g.outgoingEdgesOf(c);
                for (Edge edge : outEdges) {
                    bw.append(c + "").append(" ").append(edge.getDestination() + "").append(" ").append(edge.getLabel() + "").append("\n");
                }
            }
            result = true;
        } catch (IOException ex) {
            throw new LoadException("Cannot write file %s", filename, ex);
        } finally {
            Utilities.close(bw);
        }

        return result;
    }

    public static <T extends Multigraph> HashMap<Long, String> loadGraph(String filename, T graph) throws ParseException, LoadException  {
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
//            Map<Long, Concept> concepts = new HashMap<Long, Concept>();
//            Concept sourceNode = null;
//            Concept destinationNode = null;
            if (graph == null) {
                throw new LoadException("Input graph cannot be null");
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
//                    try {
//                        sourceId = Long.parseLong(source);
//                        destId = Long.parseLong(dest);
//                        edgeLabel = Long.parseLong(label);
//                    } catch (NumberFormatException nfex) {
//                        sourceId = FreebaseConstants.convertMidToLong(source);
//                        destId = FreebaseConstants.convertMidToLong(dest);
//                        edgeLabel = Edge.GENERIC_EDGE_LABEL;
//                        if (label != null) {
//                            if (label.equals("isA")) {
//                                edgeLabel = FreebaseConstants.ISA_ID;
//                            } else {
//                                //LinkedList<String> edgeLabelName = new LinkedList<String>();
//                                //edgeLabelName.add(label);
//
//                                //Map<String, String> mid;
//
//                                edgeLabel = FreebaseConstants.getPropertyId(label);
//                                //edgeLabel = FreebaseConstants.convertMidToLong(mid.get(label));
//                            }
//                        }
//                    }



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
                    graph.addVertex(sourceId);
                    graph.addVertex(destId);
                    graph.addEdge(sourceId, destId, edgeLabel);
                }
            }

        } catch (IOException ex) {
            Logger.getLogger(GraphFilesManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return midLabels;
    }

    public static Multigraph loadGraph(String filename) throws ParseException, LoadException {
        BaseMultigraph graph = new BaseMultigraph();
        loadGraph(filename, graph);
        return graph;
    }


    public static HashMap<Long, String> getMidLabels(String filename) throws ParseException, LoadException{
        HashMap<Long, String> midLabels = new HashMap<Long, String>();
        try {
            File file = new File(filename);
            BufferedReader in = new BufferedReader(new FileReader(file));

            long edgeLabel;
            String line, source, dest, label;
            String[] tokens;
            int count = 0;

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
                    label = tokens[2];
                    try {
                        edgeLabel = Long.parseLong(label);
                    } catch (NumberFormatException nfex) {
                        edgeLabel = Edge.GENERIC_EDGE_LABEL;
                        if (label != null) {
                            if (label.equals("isA")) {
                                edgeLabel = FreebaseConstants.ISA_ID;
                            } else {
                                //LinkedList<String> edgeLabelName = new LinkedList<String>();
                                //edgeLabelName.add(label);

                                //Map<String, String> mid;

                                edgeLabel = FreebaseConstants.getPropertyId(label);
                                //edgeLabel = FreebaseConstants.convertMidToLong(mid.get(label));
                            }
                            midLabels.put(edgeLabel, label);
                        }
                    }
                }
            }

        } catch (IOException ex) {
            Logger.getLogger(GraphFilesManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return midLabels;
    }

    /**
     * Writes into file a graph using the popular Pajek NET Extended As for
     * GraphInsight application. Nodes will be labeled with almost-progressive
     * ID, URL to MID Arcs will have weight 1, while isA will have weight 10
     * Metadata will be appended. For compactness we will subtract to each node
     * id the (minimum-1) in the set That value will be printed in the file
     * second line Like the following:
     *
     * %META Metadata here %11235813 %*Colnames "URL_MID"
     *
     * *Vertices 1 "/m/0fmngy" 2 "/m/03c2462" 3 "/m/01m1_t" ...
     *
     * *Arcs 1 2 1 1 2 1 1 3 4
     *
     * @param filename the filename to which save the graph
     * @param g the graph
     * @param metadata additional informations
     * @throws LoadException
     */
    public static boolean exportGraphToNET(String filename, Multigraph g, String metadata) throws LoadException {
        return exportGraphToNET(filename, g, metadata, false);
    }

    public static boolean exportGraphToNET(String filename, Multigraph g, String metadata, boolean onlyISA) throws LoadException {
        boolean result = false;
        TreeMap<Long, Integer> idMap = new TreeMap<Long, Integer>();



        if (g == null) {
            throw new LoadException("Graph cannot be null");
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
                    bw.append(idMap.get(c) + " \"http://www.freebase.com" + FreebaseConstants.convertLongToMid(c) + "\"\n");
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
            throw new LoadException("Cannot write file %s", filename, ex);
        } finally {
            Utilities.close(bw);
        }

        return result;
    }
}

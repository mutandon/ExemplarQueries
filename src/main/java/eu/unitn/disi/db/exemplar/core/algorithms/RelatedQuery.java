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
package eu.unitn.disi.db.exemplar.core.algorithms;

import eu.unitn.disi.db.command.Interruptible;
import eu.unitn.disi.db.command.util.LoggableObject;
import eu.unitn.disi.db.grava.exceptions.DataException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * This class contains a naive implementation for Algorithm1 thus the solution
 * obtained traversing the graph in order to find subgraphs matching the pattern
 * in the query given as input
 *
 * @author Matteo Lissandrini <matteo.lissandrini at gmail.com>
 */
//TODO: Use algorithm notation.
public abstract class RelatedQuery extends LoggableObject implements Comparable, Interruptible {

    public final static double MIN_SIMILARITY = 0.5;
    //private static Multigraph knowledgeBaseSubgraph;
    /**
     * Query node to Graph node map
     */
    protected Map<Long, Long> mappedConcepts;
    protected Map<Long, Long> reversedMappedConcepts;
    /**
     * Query edge to Graph edge map
     */
    protected Map<Edge, Edge> mappedEdges;
    protected Map<Long, Double> nodeWeights;
    protected Set<String> usedEdges;
    protected Multigraph query;
    protected double totalWeight;
    protected static ArrayList<Future<List<RelatedQuery>>> lists;
    protected static ExecutorService pool;

    RelatedQuery() {  } //A default constructor used for serialization

    /**
     * Constructor for the class
     *
     * @param query the query that needs to be mapped
     */
    public RelatedQuery(Multigraph query) {
        this.query = query;
        this.initialize();
    }

    private void initialize() {
        this.mappedConcepts = new HashMap<>(query.vertexSet().size() + 2, 1.5f);
        this.reversedMappedConcepts = new HashMap<>(query.vertexSet().size() + 2, 1.5f);
        this.usedEdges = new HashSet<>(query.edgeSet().size() + 2, 1.5f);
        for (Long c : query.vertexSet()) {
            this.mappedConcepts.put(c, null);
        }


        this.mappedEdges = new HashMap<>(query.edgeSet().size() + 2, 1.5f);
        for (Edge l : query.edgeSet()) {
            this.mappedEdges.put(l, null);
        }

        totalWeight = 0;
        nodeWeights = new HashMap<>();
    }

    public void clear(){
        this.initialize();
    }

    public Set<Long> getUsedConcepts() {
        return reversedMappedConcepts.keySet();
    }


    /**
     * check whether the queryConcept has already been mapped to a KnowledgeBase
     * Concept
     *
     * @param queryConcept
     * @return true if the queryConcpet has already been mapped to a
     * KnowledgeBAse Concept
     */
    public boolean hasMapped(Long queryConcept) {
        return this.mappedConcepts.containsKey(queryConcept) && this.mappedConcepts.get(queryConcept) != null;
    }

    /**
     * check whether the kbConcept has already been mapped to a queryConcept
     *
     * @param kbConcept
     * @return true if the kbConcpet has already been mapped to a queryConcept
     */
    public boolean isUsing(Long kbConcept) {
        return this.reversedMappedConcepts.containsKey(kbConcept) && this.reversedMappedConcepts.get(kbConcept) != null;
    }



    public Long mappedAs(Long kbConcept) {
        return  this.reversedMappedConcepts.get(kbConcept);
    }

    public Long mapOf(Long queryConcept) {
        return this.mappedConcepts.get(queryConcept);
    }

    /**
     * Maps the passed query concept to the given concept in the knowledgebase
     *
     * @param queryConcept
     * @param kbConcept
     */
    public void map(Long queryConcept, Long kbConcept) {
        if (this.mappedConcepts.containsKey(queryConcept)) {
            mappedConcepts.put(queryConcept, kbConcept);
            reversedMappedConcepts.put(kbConcept, queryConcept);
        } else {
            throw new IllegalArgumentException("Query concept " + queryConcept + " is not present");
        }

    }

    /**
     * check whether the queryEdge has already been mapped to a KnowledgeBase
     * Edge
     *
     * @param queryEdge
     * @return true if the queryConcpet has already been mapped to a
     * KnowledgeBAse Concept
     */
    public boolean hasMapped(Edge queryEdge) {
        return this.mappedEdges.containsKey(queryEdge) && this.mappedEdges.get(queryEdge) != null;
    }

    /**
     * check whether the kbEdge has already been mapped to a queryEdge
     *
     * @param kbEdge
     * @return true if the kbEdge has already been mapped to a queryEdge
     */
    public boolean isUsing(Edge kbEdge) {
        return this.usedEdges.contains(kbEdge.getId());
    }

    /**
     * Maps the passed query edge to the given edge in the knowledgebase
     *
     * @param queryEdge
     * @param kbEdge
     */
    public void map(Edge queryEdge, Edge kbEdge) {
        if (this.mappedEdges.containsKey(queryEdge)) {
            mappedEdges.put(queryEdge, kbEdge);
            usedEdges.add(kbEdge.getId());
        } else {
            throw new IllegalArgumentException("Query edge" + queryEdge + " is not present");
        }
    }

    public Edge mapOf(Edge queryEdge) {
        return this.mappedEdges.get(queryEdge);
    }

    /**
     * Try to build a graph from the mapped concepts and edges
     *
     * @return
     */
    public Multigraph buildRelatedQueryGraph() {
        Multigraph queryGraph = new BaseMultigraph();

        for (Long c : this.mappedConcepts.values()) {
            if (c == null) {
                throw new IllegalStateException("The query is not totally mapped: missing a node");
            }
            queryGraph.addVertex(c);
        }

        for (Edge l : this.mappedEdges.values()) {
            if (l == null) {
                throw new IllegalStateException("The query is not totally mapped: missing an edge");
            }
            smartAddEdge(queryGraph, l);
        }
        return queryGraph;
    }

//    public DirectedMultigraph<Concept, LabeledEdge> getTargetQuery() {
//        return this.targetQuery;
//    }
    /**
     * Returns a <b>copy</b> of the set of mapped Concepts
     *
     * @return the set of mapped Concepts
     */
    public HashMap<Long, Long> getMappedConcepts() {
        HashMap<Long, Long> m = new HashMap<>(this.mappedConcepts.size() + 2, 1.5f);
        m.putAll(this.mappedConcepts);
        return m;
    }

    /**
     * Returns a <b>copy</b> of the set of mapped Edges
     *
     * @return the set of mapped Edges
     */
    public Map<Edge, Edge> getMappedEdges() {
        Map<Edge, Edge> m = new HashMap<Edge, Edge>();
        m.putAll(this.mappedEdges);
        return m;
    }

    public Set<String> getUsedEdges() {
        Set<String> m = new HashSet<String>();
        m.addAll(this.usedEdges);
        return m;
    }
    
    public Map<Long, Long> getRevesedMappedConcepts() {
        Map<Long, Long> m = new HashMap<Long, Long>(this.mappedConcepts.size() + 2, 1.5f);
        m.putAll(this.reversedMappedConcepts);
        return m;
    }

    public void setMappedConcepts(Map<Long, Long> mappedConcepts) {
        this.mappedConcepts = mappedConcepts;
    }

    public void setMappedEdges(Map<Edge, Edge> mappedEdges) {
        if(mappedEdges!=null && mappedEdges.size()>0){
            this.mappedEdges.putAll(mappedEdges);
        }
    }

    public void setUsedEdges(Set<String> used) {
            this.usedEdges.addAll(used);
    }

    public void setReversedMappedConcepts(Map<Long, Long> reversedMap){
        this.reversedMappedConcepts = reversedMap;
    }


    ///======================= START STATIC METHODS
    protected static RelatedQuery loggable;


    /**
     * traverse the portion of the graph selected in order to find subgraphs
     * matching the pattern in the query given as input
     *
     * @param query the input query
     * @param kbs the portion of the knowledgebase used as search pool
     * @return the list of possible related queries
     */
    public List<RelatedQuery> findRelated(Multigraph query, Multigraph kbs) throws DataException {
        return findRelated(query, kbs, null);
    }

    public abstract List<RelatedQuery> findRelated(Multigraph query, Multigraph kbs, Map<Long,Set<Long>> queryGraphMap) throws DataException;

    /**
     * Given a query graph it return the node that is more likely to be the root
     * of it
     *
     * @param query the query graph
     * @param kbEdges
     * @return the (likely-to-be) node
     */
    public static Long getRootNode(Multigraph query, Collection<Edge> kbEdges) {
        Collection<Long> nodes = query.vertexSet();

        Set<Long> edgeLabels = new HashSet<>();
        for (Edge l : query.edgeSet()) {
            edgeLabels.add(l.getLabel());
        }


        Long bestLabel = findLessFrequentLabel(kbEdges, edgeLabels);
        Collection<Edge> edgesIn, edgesOut;

        for (Long concept : nodes) {
            edgesIn = query.incomingEdgesOf(concept);
            for (Edge labeledEdge : edgesIn) {
                if (labeledEdge.getLabel() == bestLabel.longValue()) {
                    return concept;
                }
            }

            edgesOut = query.outgoingEdgesOf(concept);
            for (Edge labeledEdge : edgesOut) {
                if (labeledEdge.getLabel() == bestLabel.longValue()) {
                    return concept;
                }
            }
        }

        return null;
    }

    public static Long findLessFrequentLabel(Collection<Edge> kbEdges, Collection<Long> allowedLabels) {
        TreeMap<Long, Integer> frequency = new TreeMap<Long, Integer>();
        int f = 0;
        int minF = -1;
        Long bestLabel = null;

        for (Edge edge : kbEdges) {
            if (allowedLabels.contains(edge.getLabel())) {
                if (frequency.containsKey(edge.getLabel())) {
                    f = frequency.get(edge.getLabel());
                    frequency.put(edge.getLabel(), f + 1);
                } else {
                    frequency.put(edge.getLabel(), 1);
                    f = 1;
                }
            }
        }

        for (Edge edge : kbEdges) {

            if (allowedLabels.contains(edge.getLabel())) {
                f = frequency.get(edge.getLabel());
                //RelatedQuery.loggable.info("%s Freq %d", edge.getId(), f );
                if (minF == -1 || f <= minF) {
                    minF = f;
                    bestLabel = edge.getLabel();
                }
            }
        }
        return bestLabel;
    }

    /**
     * Checks if, for a given concept, it exist an <b>outgoing</b> with that
     * label and returns all the edges found
     *
     * @param label the label we are looking for
     * @param kbEdges the knoweldgebase edges
     * @return labeled edges, can be empty
     */
    public static List<Edge> findLabeledEdges(Long label, Set<Edge> kbEdges) {

        // Compare to the edges in the KB exiting from the mapped concept passed
        List<Edge> edges = new ArrayList<>();

        for (Edge labeledEdge : kbEdges) {
            if (label == Edge.GENERIC_EDGE_LABEL || labeledEdge.getLabel() == label) {
                edges.add(labeledEdge);
            }
        }
        return edges;
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

    public static Multigraph smartAddEdge(Multigraph graph, Edge edge) {

        if (!graph.containsVertex(edge.getSource())) {
            graph.addVertex(edge.getSource());
        }
        if (!graph.containsVertex(edge.getDestination())) {
            graph.addVertex(edge.getDestination());
        }
        //That a god mapping edge, add to the related query
        graph.addEdge(edge.getSource(), edge.getDestination(), edge.getLabel());
        return graph;
    }

    public double addWeight(Long c, Double w) {
        Double old;
        old = nodeWeights.put(c, w);
        if (old != null) {
            totalWeight -= old;
            totalWeight += w;
        } else {
            totalWeight += w;
        }

        return totalWeight;
    }
    
    public double getTotalWeight() {
        return totalWeight;
    }

    @Override
    public int compareTo(Object obj) {

        if (obj == null) {
            throw new NullPointerException("Null object cannot be compared");
        }

        if (getClass() != obj.getClass()) {
            throw new ClassCastException("Only RelatedQuery objects can be compared");
        }

        RelatedQuery other = (RelatedQuery) obj;
        return (new Double(this.totalWeight)).compareTo(new Double(other.getTotalWeight()));
    }


    @Override
    public boolean equals(Object obj){
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RelatedQuery other = (RelatedQuery) obj;

        Map<Long, Long> otherConcepts = other.getMappedConcepts();

        for (Long toCheck : this.mappedConcepts.keySet()) {
            if(!otherConcepts.containsKey(toCheck)) {
                return false;
            }
            if( !otherConcepts.get(toCheck).equals( this.mappedConcepts.get(toCheck))){
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString(){
        String s = "";
        for (String e : usedEdges) {
            s+=e+" ";
        }
        return s;
    }

    @Override
    public int hashCode(){
        return this.mappedEdges.hashCode();
    }

}

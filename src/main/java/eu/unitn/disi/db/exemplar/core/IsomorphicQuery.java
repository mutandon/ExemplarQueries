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
package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class extends the RelatedQuery class with the Isomorphic Query thus the
 * mapping between query nodes and graph nodes is enforced to bijective
 *
 * @author Matteo Lissandrini <ml at disi.unitn.eu>
 */
public class IsomorphicQuery extends RelatedQuery {

    /**
     * Query node to Graph node map
     */
    protected Map<Long, Long> mappedNodes;
    protected Map<Long, Long> reversedMappedNodes;
    /**
     * Query edge to Graph edge map
     */
    protected Map<Edge, Edge> mappedEdges;
    protected Set<String> usedEdgesIDs; // TODO: This is not a very good idea

    IsomorphicQuery() {
    } //A default constructor used for serialization

    /**
     * Constructor for the class
     *
     * @param query the query that needs to be mapped
     */
    public IsomorphicQuery(Multigraph query) {
        super(query);
        this.initialize();
    }

    private void initialize() {
        this.mappedNodes = new HashMap<>(query.vertexSet().size() + 2, 1f);
        this.reversedMappedNodes = new HashMap<>(query.vertexSet().size() + 2, 1f);

        for (Long n : query.vertexSet()) {
            this.mappedNodes.put(n, null);
        }

        this.usedEdgesIDs = new HashSet<>(query.edgeSet().size() + 2, 1f);
        this.mappedEdges = new HashMap<>(query.edgeSet().size() + 2, 1f);

        for (Edge e : query.edgeSet()) {
            this.mappedEdges.put(e, null);
        }
    }

    @Override
    public IsomorphicQuery getClone() {
        IsomorphicQuery clone = new IsomorphicQuery(this.query);
        clone.mappedNodes = new HashMap<>(query.vertexSet().size() + 2, 1f);
        clone.mappedNodes.putAll(this.mappedNodes);

        clone.reversedMappedNodes = new HashMap<>(query.vertexSet().size() + 2, 1f);
        clone.reversedMappedNodes.putAll(this.reversedMappedNodes);

        clone.usedEdgesIDs = new HashSet<>(query.edgeSet().size() + 2, 1f);
        clone.usedEdgesIDs.addAll(this.usedEdgesIDs);

        clone.mappedEdges = new HashMap<>(query.edgeSet().size() + 2, 1f);
        clone.mappedEdges.putAll(this.mappedEdges);

        clone.totalWeight = this.totalWeight;
        clone.nodeWeights = new HashMap<>();
        clone.nodeWeights.putAll(this.nodeWeights);

        return clone;
    }

    /**
     * Maps the passed query node to the given node in the graph
     *
     * @param queryNode
     * @param graphNode
     */
    @Override
    public void map(Long queryNode, Long graphNode) {
        if (queryNode == null) {
            throw new IllegalArgumentException("Query Node cannot be null");
        }

        if (graphNode == null) {
            throw new IllegalArgumentException("Graph Node cannot be null");
        }

        if (!this.mappedNodes.containsKey(queryNode)) {
            throw new IllegalArgumentException("Query node " + queryNode + " is not present in the original query");
        } else if (mappedNodes.get(queryNode) == null) {
            mappedNodes.put(queryNode, graphNode);
            reversedMappedNodes.put(graphNode, queryNode);
        } else if (!this.mappedNodes.get(queryNode).equals(graphNode)) {
            throw new IllegalArgumentException("Trying to change map of query node " + queryNode + " a different map is already present");
        }
    }

    /**
     * Maps the passed query edge to the given edge in the graph
     *
     * @param queryEdge
     * @param graphEdge
     */
    @Override
    public void map(Edge queryEdge, Edge graphEdge) {
        if (queryEdge == null) {
            throw new IllegalArgumentException("Query Edge cannot be null");
        }

        if (graphEdge == null) {
            throw new IllegalArgumentException("Graph Edge cannot be null");
        }

        if (!this.mappedEdges.containsKey(queryEdge)) {
            throw new IllegalArgumentException("Query edge " + queryEdge + " is not present in the original query");
        } else if (mappedEdges.get(queryEdge) == null) {
            mappedEdges.put(queryEdge, graphEdge);
            usedEdgesIDs.add(graphEdge.getId());
        } else if (!this.mappedEdges.get(queryEdge).equals(graphEdge)) {
            throw new IllegalArgumentException("Trying to change map of query edge " + queryEdge + " a different map is already present");
        }
    }

    /**
     *
     * @return the list of graph nodes mapped to something
     */
    @Override
    public Set<Long> getUsedNodes() {
        return reversedMappedNodes.keySet();
    }

    /**
     *
     * @return a copy of the map from query nodes to graph node
     */
    public Map<Long, Long> getNodesMapping() {
        Map<Long, Long> m = new HashMap<>();
        m.putAll(this.mappedNodes);
        return m;
    }

    /**
     * check whether the queryNode has already been mapped to a Graph Node
     *
     * @param queryNode
     * @return true if the queryNode has been mapped to a graph node
     */
    @Override
    public boolean hasMapped(Long queryNode) {
        return this.mappedNodes.containsKey(queryNode) && this.mappedNodes.get(queryNode) != null;
    }

    /**
     * check whether the GraphNode has been mapped to a queryNode
     *
     * @param graphNode
     * @return true if the graphNode has been mapped to a queryNode
     */
    @Override
    public boolean isUsing(Long graphNode) {
        return this.reversedMappedNodes.containsKey(graphNode) && this.reversedMappedNodes.get(graphNode) != null;
    }

    /**
     * check whether the queryEdge has been mapped to a graph Edge
     *
     * @param queryEdge
     * @return true if the queryEdge has been mapped to a graph Edge
     */
    @Override
    public boolean hasMapped(Edge queryEdge) {
        return this.mappedEdges.containsKey(queryEdge) && this.mappedEdges.get(queryEdge) != null;
    }

    /**
     * check whether the graphEdge has been mapped to a queryEdge
     *
     * @param graphEdge
     * @return true if the graphEdge has already been mapped to a queryEdge
     */
    @Override
    public boolean isUsing(Edge graphEdge) {
        return this.usedEdgesIDs.contains(graphEdge.getId());
    }

    /**
     *
     * @return a copy of the list of graph edges in use
     */
    @Override
    public Set<String> getUsedEdgesIDs() {
        Set<String> m = new HashSet<>();
        m.addAll(this.usedEdgesIDs);
        return m;
    }

    /**
     *
     * @return a copy of the list of graph edges in use
     */
    @Override
    public Set<Edge> getUsedEdges() {
        Set<Edge> m = new HashSet<>();
        m.addAll(this.mappedEdges.values());
        return m;
    }

    /**
     *
     * @param graphNode
     * @return the single node mapping to this graph node
     */
    public Long mappedAs(Long graphNode) {
        return this.reversedMappedNodes.get(graphNode);
    }

    /**
     *
     * @param queryNode
     * @return the only single node mapped to this query node
     */
    public Long isomorphicMapOf(Long queryNode) {
        return this.mappedNodes.get(queryNode);
    }

    @Override
    public List<Long> mapOf(Long queryNode) {
        List<Long> map = new LinkedList<>();
        map.add(this.mappedNodes.get(queryNode));
        return map;
    }

    @Override
    public List<Edge> mapOf(Edge queryEdge) {
        List<Edge> map = new LinkedList<>();
        map.add(this.mappedEdges.get(queryEdge));
        return map;
    }

    /**
     *
     * @param queryEdge
     * @return the only single Edge mapped to this query edge
     */
    public Edge ismorphicMapOf(Edge queryEdge) {
        return this.mappedEdges.get(queryEdge);
    }

    /**
     * Try to build a graph from the mapped concepts and edges
     *
     * @return
     */
    @Override
    public Multigraph buildRelatedQueryGraph() {
        Multigraph queryGraph = new BaseMultigraph();

        for (Long node : this.mappedNodes.values()) {
            if (node == null) {
                throw new IllegalStateException("The query is not totally mapped: missing a node");
            }

            queryGraph.addVertex(node);
        }

        for (Edge edge : this.mappedEdges.values()) {
            if (edge == null) {
                throw new IllegalStateException("The query is not totally mapped: missing an edge");
            }

            smartAddEdge(queryGraph, edge, true);
        }
        return queryGraph;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        if (obj instanceof IsomorphicQuery) {
            final IsomorphicQuery other = (IsomorphicQuery) obj;

            Map<Long, Long> otherNodes = other.getNodesMapping();

//            for (Long toCheck : this.mappedNodes.keySet()) {
//                if (!otherNodes.containsKey(toCheck)) {
//                    return false;
//                }
//
//                if (!otherNodes.get(toCheck).equals(this.mappedNodes.get(toCheck))) {
//                    return false;
//                }
//            }

            Set<String> otherEdges = other.getUsedEdgesIDs();

            if (otherEdges.size() == this.usedEdgesIDs.size()) {
                for (String otherEdge : otherEdges) {
                    if (!this.usedEdgesIDs.contains(otherEdge)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        String s = "";
        for (String e : usedEdgesIDs) {
            s += e + " ";
        }
        return s;
    }

    @Override
    public int hashCode() {
        return this.usedEdgesIDs.hashCode();
    }

}

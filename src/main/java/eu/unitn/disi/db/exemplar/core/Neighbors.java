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
package eu.unitn.disi.db.exemplar.core;

import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import java.util.Iterator;

/**
 * Represent the neighbors of a node. The advantage of this class is that you can easily
 * parallelize the operations if you have the indegree and outdegree.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class Neighbors implements Iterable<Edge> {
    private long source;
    private long[] incomingNodes;
    private long[] incomingEdges;
    private long[] outgoingNodes;
    private long[] outgoingEdges;

    public Neighbors(long source, int inDegree, int outDegree) {
        if (inDegree > 0) {
            incomingNodes = new long[inDegree];
            incomingEdges = new long[inDegree];
        }
        if (outDegree > 0) {
            outgoingNodes = new long[outDegree];
            outgoingEdges = new long[outDegree];
        }
        this.source = source;
    }

    public long[] getIncomingEdges() {
        return incomingEdges;
    }

    public long[] getIncomingNodes() {
        return incomingNodes;
    }

    public long[] getOutgoingEdges() {
        return outgoingEdges;
    }

    public long[] getOutgoingNodes() {
        return outgoingNodes;
    }

    public long getSource() {
        return source;
    }

    public BaseMultigraph toGraph() {
        BaseMultigraph multigraph = new BaseMultigraph();
        multigraph.addVertex(source);
        Long dest;
        for (int i = 0; i < incomingNodes.length; i++) {
            dest = incomingNodes[i];
            multigraph.addVertex(dest);
            multigraph.addEdge(dest, source, incomingEdges[i]);
        }
        for (int i = 0; i < outgoingNodes.length; i++) {
            dest = outgoingNodes[i];
            multigraph.addVertex(dest);
            multigraph.addEdge(source, dest, outgoingEdges[i]);
        }
        return multigraph;
    }

    @Override
    public Iterator<Edge> iterator() {
        return new NeighborsIterator();
    }

    private class NeighborsIterator implements Iterator<Edge> {
        private int count;

        public NeighborsIterator() {
            count = -1;
        }

        @Override
        public boolean hasNext() {
            return count < (incomingNodes.length + outgoingNodes.length);
        }

        @Override
        public Edge next() {
            Edge edge = null;

            count++;
            if (count < incomingNodes.length) {
                edge = new Edge(incomingNodes[count], source, incomingEdges[count]);
            } else if (hasNext()) {
                int index = count % incomingNodes.length;
                edge = new Edge(source, outgoingNodes[index], outgoingEdges[index]);
            }
            return edge;
        }

        @Override
        public void remove() {
            if (count >= 0) {
                long[] newArray;
                if (count < incomingNodes.length) {
                    newArray = new long[incomingNodes.length - 1];
                    System.arraycopy(incomingNodes, 0, newArray, 0, count);
                    System.arraycopy(incomingNodes, count + 1, newArray, count, incomingNodes.length - count);
                    incomingNodes = newArray;
                    System.arraycopy(incomingEdges, 0, newArray, 0, count);
                    System.arraycopy(incomingEdges, count + 1, newArray, count, incomingNodes.length - count);
                    incomingEdges = newArray;
                } else if (hasNext()) {
                    int index = count % incomingNodes.length;
                    newArray = new long[incomingNodes.length - 1];
                    System.arraycopy(outgoingNodes, 0, newArray, 0, index);
                    System.arraycopy(outgoingNodes, index + 1, newArray, index, outgoingNodes.length - index);
                    outgoingNodes = newArray;
                    System.arraycopy(outgoingEdges, 0, newArray, 0, index);
                    System.arraycopy(outgoingEdges, index + 1, newArray, index, incomingNodes.length - index);
                    outgoingEdges = newArray;
                }
            }
        }
    }
}

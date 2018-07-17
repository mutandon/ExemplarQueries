/*
 * The MIT License
 *
 * Copyright 2016 Matteo Lissandrini <ml@disi.unitn.eu>.
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
package eu.unitn.disi.db.exemplar.core.algorithms;

import eu.unitn.disi.db.command.algorithmic.Algorithm;
import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import java.util.Set;

/**
 * Given a Graph  computes the set of connected components
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class ConnectedComponents extends Algorithm{

    

    @AlgorithmInput
    private Multigraph forest;

    
    @AlgorithmInput
    private List<Multigraph> trees;

    
    @Override
    protected void algorithm() throws AlgorithmExecutionException {
        final int INIT_SIZE = forest.numberOfNodes();
        trees = new ArrayList<>(INIT_SIZE);

        Set<Long> visited = new HashSet<>(INIT_SIZE*(4/3));

        BaseMultigraph tmp;
        Collection<Edge> toAdd;
        Collection<Edge> nextAdd;
        for (long node : forest) {
            if (!visited.contains(node)) {
                // Build new with BFS
                tmp = new BaseMultigraph(INIT_SIZE);
                toAdd = forest.edgesOf(node);
                visited.add(node);
                while (!toAdd.isEmpty()) {
                    nextAdd = new HashSet<>(INIT_SIZE);
                    for (Edge e : toAdd) {
                        tmp.forceAddEdge(e);
                        //Expand
                        for (int it = 0; it < 2; it++) {
                            long tNode = it == 0 ? e.getSource() : e.getDestination();
                            if (!visited.contains(tNode)) {
                                visited.add(tNode);
                                nextAdd.addAll(forest.edgesOf(tNode));
                            }
                        }

                    }
                    toAdd = nextAdd;
                }
                trees.add(tmp);
            }
        }
    }

    

    public void setForest(Multigraph forest) {
        this.forest = forest;
    }

    public List<Multigraph> getTrees() {
        return trees;
    }

    /**
     * 
     * @param g the query graph
     * @return connected components in the query
     * TODO: Merge with above
     */
    public static List<Multigraph> splitGraph(Multigraph g) {
        return splitGraph(g, false);
        
    }
    
     /**
     * 
     * @param g the query graph
     * @param excludeSingle
     * @return connected components in the query
     * TODO: Merge with above
     */
    public static List<Multigraph> splitGraph(Multigraph g, boolean excludeSingle) {
        final int INIT_SIZE = g.numberOfNodes();
        List<Multigraph> queries = new ArrayList<>(INIT_SIZE);

        Set<Long> visited = new HashSet<>(INIT_SIZE);

        BaseMultigraph tmp;
        Collection<Edge> toAdd;
        Collection<Edge> nextAdd;
        for (long node : g) {
            if(excludeSingle && g.degreeOf(node)<1){
                continue;
            }
            if (!visited.contains(node)) {
                // Build new with BFS
                tmp = new BaseMultigraph(INIT_SIZE);
                toAdd = g.edgesOf(node);
                visited.add(node);
                while (!toAdd.isEmpty()) {
                    nextAdd = new HashSet<>(INIT_SIZE);
                    for (Edge e : toAdd) {
                        tmp.forceAddEdge(e);
                        //Expand
                        for (int it = 0; it < 2; it++) {
                            long tNode = it == 0 ? e.getSource() : e.getDestination();
                            if (!visited.contains(tNode)) {
                                visited.add(tNode);
                                nextAdd.addAll(g.edgesOf(tNode));
                            }
                        }

                    }
                    toAdd = nextAdd;
                }
                queries.add(tmp);
            }
        }

        return queries;
    }
    
    
    
}

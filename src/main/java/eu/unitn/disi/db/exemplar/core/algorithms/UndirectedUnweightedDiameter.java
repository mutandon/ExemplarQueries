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
import eu.unitn.disi.db.command.algorithmic.AlgorithmOutput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class UndirectedUnweightedDiameter extends Algorithm{

    @AlgorithmInput
    protected Multigraph graph;
    
    @AlgorithmOutput
    protected int diameter;
    
    
    @Override
    protected void algorithm() throws AlgorithmExecutionException {
        diameter = 0;
        int max = 0;        
        int numNodes = graph.numberOfNodes();
        final long DELIM =-1l;
        for(Long current: graph.vertexSet()){
            Queue<Long> toVisit = new LinkedList<>();
            toVisit.add(current);
            
            
            Set<Long> visited = new HashSet<>(numNodes*4/3);
            visited.add(current);
            
            long node, next;
            Iterator<Edge> aEdges;
            Edge e;
            
            toVisit.add(DELIM);
            int localDim = -1;
            while(!toVisit.isEmpty()){
                node = toVisit.poll();
                if(node==DELIM){
                    localDim++;
                    if(!toVisit.isEmpty()){
                        toVisit.add(DELIM);
                    }                    
                    continue;
                }
                for (int i = 0; i < 2; i++) {
                    aEdges = i == 0 ? graph.incomingEdgesIteratorOf(node) : graph.outgoingEdgesIteratorOf(node);
                    while (aEdges.hasNext()) {
                        e = aEdges.next();
                        next = i == 0 ? e.getSource() : e.getDestination();
                        if(!visited.contains(next)){
                           toVisit.add(next);
                           visited.add(next);
                        }
                    }
                }                
            }          
            if(localDim > diameter){
              diameter = localDim;  
            }
        }
        
        
        
        
    }


    public void setGraph(Multigraph graph) {
        this.graph = graph;
    }

    public int getDiameter() {
        return diameter;
    }

    
    
}

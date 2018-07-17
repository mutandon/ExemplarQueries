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
package eu.unitn.disi.db.exemplar.multiple.core;

import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class JointNode {

    private final long node;
    private final ArrayList<ArrayList<Pair<Integer, Multigraph>>> matches;

    public JointNode(long node, int samples) {
        this.node = node;
        this.matches = new ArrayList<>(samples);
        for (int i = 0; i < samples; i++) {
            this.matches.add(new ArrayList<>());
        }

    }

    public long getNode(){
        return this.node;
    }
    
    public void addMatch(int sampleId, int answerId, Multigraph ans) {
        this.matches.get(sampleId).add(new Pair<>(answerId, ans));
    }
    
    public boolean hasMatch(int sampleId) {
        return !this.matches.get(sampleId).isEmpty();
    }
    
    public Collection<? extends Pair<Integer, Multigraph>> getMatches(int sampleId) {
        return this.matches.get(sampleId);
    }

    public boolean isComplete() {

        for (ArrayList<Pair<Integer, Multigraph>> matche : matches) {
            if (matche.isEmpty()) {
                return false;
            }
        }
        return true;

    }

    public List<Integer> matches() {
        List<Integer> ms = new ArrayList<>();
        int i =0;
        for (ArrayList<Pair<Integer, Multigraph>> matche : matches) {            
            if (!matche.isEmpty()) {
                ms.add(i);
            }
            i++;
        }
        return ms;

    }

    
    public boolean isJoint() {
        int count =0;
        for (ArrayList<Pair<Integer, Multigraph>> matche : matches) {
            if(!matche.isEmpty()){
                count++;
                if(count>1){
                    return true;
                }
            }
        }
        return false;

    }
    
    @Override
    public String toString(){
        String out = "[ ";
        int sId = 0;
        for (ArrayList<Pair<Integer, Multigraph>> matche : matches) {
            out+=sId+ ": " + (( matche!= null) ? matche.size() : "0" )+ " -";
            sId++;
        }
        return out+ " ]";
                
    }
    
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + (int) (this.node ^ (this.node >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JointNode other = (JointNode) obj;
        if (this.node != other.node) {
            return false;
        } else if (this.matches.size() != other.matches.size()){
            return false;
        }
        return true;
    }
}

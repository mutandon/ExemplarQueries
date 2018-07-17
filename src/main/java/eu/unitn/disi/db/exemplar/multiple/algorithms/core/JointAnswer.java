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

import eu.unitn.disi.db.grava.graphs.BaseMultigraph;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * A Multiple Answer is made of multiple Exemplar Answers joined together
 * On for each fragment
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class JointAnswer implements Comparable<JointAnswer> {
    
    
    private final ArrayList<Multigraph> matches;
    private Double score = 0.0;
    
    public JointAnswer(int numSamples) {
        this.matches = new ArrayList<>(numSamples);
        for (int i = 0; i < numSamples; i++) {
            this.matches.add(null);
        }
    }
    
    
    public void addMatch(int sampleId, Multigraph ans){
        this.matches.set(sampleId,  ans);
    }
    
    

    public boolean isComplete() {
        for (int i = 0; i < matches.size(); i++) {
             if(matches.get(i) ==null){
                 return false;
             }            
        }
        return true;
    }
    
    public Set<Long> nodeSet(){
        HashSet<Long> nodes = new HashSet<>(10*matches.size());
        Multigraph ans;
        for (int i = 0; i < matches.size(); i++) {
            ans = matches.get(i);
             if( ans !=null){
                 nodes.addAll(ans.vertexSet());
             }            
        }
        
        return nodes;
        
    }

    public Set<Integer> getMappedSamples(){
        Set<Integer> mapped = new HashSet<>();
        for (int i = 0; i < matches.size(); i++) {
            if(hasMapped(i)){
                mapped.add(i);
            }            
        }
        return mapped;
    }
    
    public boolean hasMapped(int i) {
        return matches.get(i)!= null;
    }
    
    
    public JointAnswer getClone(){
        int ss = this.matches.size();
        
        JointAnswer c = new JointAnswer(ss);
        for (int i = 0; i < ss; i++) {
            c.matches.set(i, this.matches.get(i));
        }
        return c;
    }

    
    @Override
    public String toString(){
        String ut = "{ ";
        int sId = 0;
        for (Multigraph matche : matches) {
            ut+= sId +":  " + (matche == null ? "_" : matche.toString())+"\t";
            sId++;
        }
        return ut+ " }";
    }

    public ArrayList<Multigraph> getFragments() {
        return matches;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        for (Multigraph matche : matches) {
            hash = 13 * hash + matche.hashCode();
        }
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
        final JointAnswer other = (JointAnswer) obj;                               
        
        return this.getEdges().equals(other.getEdges());
    }

    public Set<Edge> getEdges() {
        HashSet<Edge> edges = new HashSet<>();       
        for (Multigraph matche : matches) {
          edges.addAll(matche.edgeSet());
        }
        return edges;
    }
    
    
    public Multigraph getGraph(){
        BaseMultigraph be = new BaseMultigraph();
        for (Multigraph matche : matches) {
          be.merge(matche);
        }
        return be;
    }

    
    public Double getScore() {
        return score;
    }

    
    public void setScore(double score){
        this.score = score;
    }

    @Override
    public int compareTo(JointAnswer o) {
        return   this.score.compareTo(o.getScore());
    }
    
    
    
    
    
    

}

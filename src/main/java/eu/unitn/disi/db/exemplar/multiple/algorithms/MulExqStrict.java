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
package eu.unitn.disi.db.exemplar.multiple.algorithms;

import eu.unitn.disi.db.command.algorithmic.Algorithm;
import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import eu.unitn.disi.db.command.algorithmic.AlgorithmOutput;
import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.multiple.core.JointAnswer;
import eu.unitn.disi.db.exemplar.multiple.core.JointNode;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.StopWatch;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Basic Algorithm: computes Multiple Exemplar Queries
 * as join of single exemplars
 * 
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class MulExqStrict extends Algorithm {

    @AlgorithmInput
    protected  List<Collection<Multigraph>> exemplarAnswers;

    @AlgorithmInput
    protected  boolean keepOnlyCount = false;

    @AlgorithmInput
    protected boolean distinct = true;


    
    @AlgorithmOutput
    protected  Collection<JointAnswer> multiAnswers;
 
    
    @AlgorithmOutput
    protected int numMulAnswers = 0;
    
    @Override
    protected void algorithm() throws AlgorithmExecutionException {
        int numQueries = exemplarAnswers.size();        
        //debug("Joining %s Samples", numQueries);
        ////gigantica 
        
        if(timeLimit > 0){
            debug("Time limit is <%ds", timeLimit);
        }
        if(memoryLimit > 0){
            info("Memory limit is <%dMb", memoryLimit);
        }
        HashMap<Long, JointNode> jointNodes = new HashMap<>(), cleaned = new HashMap<>();
        int sampleIdx =0;
        int answerIdx;
        int minIdx = 0;
        StopWatch watch = new StopWatch();
        int minSize = keepOnlyCount ? 1 : Integer.MAX_VALUE;
        JointNode matches;
        for(Collection<Multigraph> answerset: exemplarAnswers){                                        
            if(minSize > answerset.size()){
                minSize = answerset.size();
                minIdx = sampleIdx;
            }            
            answerIdx =0;
            for (Multigraph ans : answerset) {
                for(long node : ans.vertexSet()){
                    matches = jointNodes.get(node);
                    if(matches == null){
                        matches = new JointNode(node, numQueries);
                        jointNodes.put(node, matches);
                    }
                    matches.addMatch(sampleIdx, answerIdx, ans);
                }
                answerIdx++;
            }
            sampleIdx++;
        }
        
        int prePrun = jointNodes.size();
        //debug("Cleaning %s NON-Joint Nodes", jointNodes.size());
        jointNodes.forEach((Long t, JointNode u) -> {            
            if(u.isJoint()){
                cleaned.put(t, u);
            }
        });
        jointNodes.clear();
        jointNodes = cleaned;
//        if(prePrun > 100){
//            debug("Remaining Joint Nodes %s over %s", jointNodes.size(), prePrun);
//        }
   
        multiAnswers =  distinct ? new HashSet<>( minSize) : new ArrayList<>(minSize);
        
        if(jointNodes.size() <=0){
            return;
        }
        
        // Iterate over each answers of one segment ( the smaller one)
        // ALGO 3 in the Paper (to be updated) 
        
        Collection<Multigraph> seedExemplar =  exemplarAnswers.get(minIdx); 
        ArrayList<JointAnswer> seedAns = new ArrayList<>(minSize);
        ArrayList<JointAnswer> tmpAns ;
        ArrayList<JointAnswer> tmpAnsNext;
        
        
//        for (int i = 0; i < exemplarAnswers.size(); i++) {
//            if(i!= minIdx){
//                exemplarAnswers.get(i).clear();
//            }                                 
//        }
        
        JointAnswer ja;
        answerIdx =0;
        for (Multigraph exemplarAnswer : seedExemplar) {
            ja = new JointAnswer(numQueries);
            ja.addMatch(minIdx, exemplarAnswer);
            answerIdx++;
            seedAns.add(ja);
        }
        
        watch.start();
        for (JointAnswer jointAnswer : seedAns) {
            tmpAns = new ArrayList<>(2);
            tmpAns.add(jointAnswer);
            while (!tmpAns.isEmpty()) {
                Runtime runtime = Runtime.getRuntime();
                
                
                
                if (this.memoryLimit > 0 && (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.) > this.memoryLimit) {
                    warn("Memory limit reached, memory used is: %.2fMb, free memory: %.2fMb. Returning the answers computed so far", (runtime.totalMemory() - runtime.freeMemory())/(1024*1024.), runtime.freeMemory()/(1024*1024.));
                    this.setMemoryExhausted();
                    return;
                }                

                if (tmpAns.size() > 5_000_000) {
                    debug("currently  expanding %s candidates", tmpAns.size());
                }
                tmpAnsNext = new ArrayList<>(tmpAns.size());
                for (JointAnswer tmpAn : tmpAns) {
                    if (this.timeLimit > 0 && watch.getElapsedTimeSecs() > this.timeLimit) {
                        warn("Time limit reached, stopping answer expansion. Returning the answers computed so far");
                        this.setInterrupted(true);
                        return;
                    }
                    if (tmpAn.isComplete()) {
                        if(keepOnlyCount){
                            numMulAnswers++;
                        } else {
                            multiAnswers.add(tmpAn);
                        }
                    } else {
                        for (int i = 0; i < numQueries; i++) {
                            if (!tmpAn.hasMapped(i) && canExpand(tmpAn, i, jointNodes)) {
                                tmpAnsNext.addAll(expand(tmpAn, i, jointNodes));
                            }
                        }
                    }
                }
                tmpAns = tmpAnsNext;

            }

        }
        
                        
    }
 
    private boolean canExpand(JointAnswer ans, int sampleId, HashMap<Long, JointNode> jointNodes){
        Set<Long> nds = ans.nodeSet();
        JointNode tm;
        for(Long n : nds ){
            tm = jointNodes.get(n);
            if(tm != null && tm.hasMatch(sampleId)){
                return true;
            }
        }
        return false;
    }
    
    private ArrayList<JointAnswer> expand(JointAnswer ans, int sampleId, HashMap<Long, JointNode> jointNodes){
        ArrayList<JointAnswer> expAnswers = new ArrayList<>();
        Set<Pair<Integer,Multigraph>> expansions = new HashSet<>();
        Set<Long> nds = ans.nodeSet();
        JointNode tm;
        for (Long nd : nds) {
            tm = jointNodes.get(nd);
            if(tm != null && tm.hasMatch(sampleId)){
                expansions.addAll(tm.getMatches(sampleId));
            }
        }
        
        //debug("Found %s expansion for sample %s ans %s", expansions.size(), sampleId, ans);
        JointAnswer expanded;
        for (Pair<Integer,Multigraph> exp : expansions ) {
            expanded = ans.getClone();
            //System.out.println("CLONE: "+expanded);
            //debug("Adding Sample %s wiht Answer %s", sampleId, exp.getFirst());
            expanded.addMatch(sampleId, exp.getSecond());
            //System.out.println("RESULT: "+expanded);
            expAnswers.add(expanded);
        }
        //debug("Computed %s expanded answers ", expAnswers.size());
        
        
        return expAnswers;
    }

    public void setExemplarAnswers(List<Collection<Multigraph>> exemplarAnswers) {
        this.exemplarAnswers = exemplarAnswers;
    }

    public Collection<JointAnswer> getMultiAnswers() {
        return multiAnswers;
    }

    public void setKeepOnlyCount(boolean keepOnlyCount) {
        this.keepOnlyCount = keepOnlyCount;
    }

    public int getNumMulAnswers() {
        if(keepOnlyCount) {
            return numMulAnswers;
        }
        
        return multiAnswers.size();        
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }
    
    
    
    
    
    
    
}

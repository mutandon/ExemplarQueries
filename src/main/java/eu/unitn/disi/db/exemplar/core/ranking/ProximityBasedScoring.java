/*
 * Copyright (C) 2016 Davide Mottin <mottin@disi.unitn.eu>
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
package eu.unitn.disi.db.exemplar.core.ranking;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.algorithms.SampleExpansionRank;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An insane score based solely on node degree
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class ProximityBasedScoring extends NodeSimilarityScoring {

    private final double threshold = 0.0001;
    private final double restartProb = 0.15;
    private Map<Long, Double> pprV = null;

    

    @Override
    protected void algorithm() throws AlgorithmExecutionException {

        SampleExpansionRank ppr = new SampleExpansionRank(bigGraph);
        ppr.setComputeNeighborhood(false);
        ppr.setLabelInformativeness(labelInformativeness);
        ppr.setMaxNumNodes(0);
        ppr.setPriorityLabels(this.sampleLables);
        ppr.setStartingNodes(queryNodes);
        ppr.setThreshold(threshold);
        ppr.setRestartProbability(restartProb);
        ppr.compute();
        pprV = ppr.getPPRVector();

    }

    @Override
    public Map<Long, Double> getNodesScoring(Collection<Long> nodesToScore) {
        timer.reset();
        timer.start();

        Map<Long, Double> nodeScoring = new ConcurrentHashMap<>((nodesToScore.size() * 4 / 3));
        nodesToScore.parallelStream().forEach(toRank -> {
            nodeScoring.put(toRank, pprV.getOrDefault(toRank, 0.0));
        });
        timer.stop();
        
        
        return nodeScoring;

    }

    public void setPprV(Map<Long, Double> pprV) {
        this.pprV = pprV;
    }

    @Override
    protected double score(BitSet node1, BitSet node2) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Double getNodeScoring(Long nodeToScore) {
        return pprV.getOrDefault(nodeToScore, 0.0);
    }
    
    @Override
    public Double getCumulativeScore(Collection<Long> nodesToScore) {
        return nodesToScore.stream().map(toRank -> {  
            return pprV.getOrDefault(toRank, 0.0);
        }).reduce((a,b)->a+b).get();     
    }
    
    
}

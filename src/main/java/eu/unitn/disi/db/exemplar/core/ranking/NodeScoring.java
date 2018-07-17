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

import eu.unitn.disi.db.command.algorithmic.Algorithm;
import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Abstract class computing the similarity in a set of nodes. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class NodeScoring extends Algorithm {

    @AlgorithmInput
    protected AggregationFunction aggFun; 
    
     @AlgorithmInput
    protected Collection<Long> queryNodes;
    
    @AlgorithmInput
    protected Map<Long, Double> labelInformativeness;

       
    @AlgorithmInput
    protected BigMultigraph bigGraph;
    
    @AlgorithmInput
    protected Set<Long> sampleLables;
     
    @AlgorithmInput
    protected Map<Long, Integer> labelsOrder;
    
    
    public void setAggregationFunction(AggregationFunction aggFun) {
        this.aggFun = aggFun;
    }

    public abstract Map<Long, Double> getNodesScoring(Collection<Long> nodesToScore);    
    
    public abstract Double getCumulativeScore(Collection<Long> nodesToScore);    
    
    public abstract Double getNodeScoring(Long nodeToScore);    
    
    
    public void setBigGraph(BigMultigraph bigGraph) {
        this.bigGraph = bigGraph;
    }

    public void setSampleLables(Set<Long> sampleLables) {
        this.sampleLables = sampleLables;
    }
    
    public void setQueryNodes(Collection<Long> queryNodes) {
        this.queryNodes = queryNodes;
    }

    public void setLabelInformativeness(Map<Long, Double> labelInformativeness) {
        this.labelInformativeness = labelInformativeness;
    }

    public void setLabelsOrder(Map<Long, Integer> labelsOrder) {
        this.labelsOrder = labelsOrder;
    }
    
    



    
}

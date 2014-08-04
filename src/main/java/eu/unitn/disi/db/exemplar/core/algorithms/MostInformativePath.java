/*
 * The MIT License
 *
 * Copyright 2014 Davide Mottin <mottin@disi.unitn.eu>.
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

import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The most informative path contains an evil formula we do not want to spread out. 
 * For some reason we feel it is good. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class MostInformativePath extends WeightedShortestPath {
    @AlgorithmInput
    private Map<Long, Double> nodePopularities; 
    @AlgorithmInput
    private double minNodeWeight; 
    @AlgorithmInput
    private double stepPenalty; 
    @AlgorithmInput
    private boolean normalize = true; 
    @AlgorithmInput
    private double alpha; 
    @AlgorithmInput
    private double beta; 

    public MostInformativePath() {
        super();
        alpha = 1; 
        beta = 1; 
        stepPenalty = 1; 
        minNodeWeight = 0.0;
    }
        
    
    @Override
    protected double distance(Long label, Long adjNode) {
        Double weight = edgeWeights.get(label);
        double distance = 0.0; 
        if (weight == null) {
            distance += alpha * minWeight;
        } else {
            distance += alpha * weight; 
        }
        weight = nodePopularities.get(adjNode);
        if (weight == null) {
            distance += beta * minNodeWeight;
        } else  {
            distance += beta * weight; 
        }
        distance += stepPenalty; 
        return distance;
    }

    @Override
    public void setEdgeWeights(Map<Long, Double> edgeWeights) {
        this.edgeWeights = new HashMap<>(edgeWeights);
        if (normalize) {
            normalizeMap(this.edgeWeights);
        }
    }
    
    public void setNodePopularities(Map<Long, Double> nodePopularities) {
        this.nodePopularities = new HashMap<>(nodePopularities);
        if (normalize) {
            normalizeMap(this.nodePopularities);
        }
    }

    public double getMinNodeWeight() {
        return minNodeWeight;
    }

    public void setMinNodeWeight(double minNodeWeight) {
        this.minNodeWeight = minNodeWeight;
    }

    public void setStepPenalty(double stepPenalty) {
        this.stepPenalty = stepPenalty;
    }

    public void setNormalize(boolean normalize) {
        this.normalize = normalize;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public void setBeta(double beta) {
        this.beta = beta;
    }
    
    private void normalizeMap(Map<Long,Double> map) {
        Set<Long> keys = map.keySet();
        double sum = 0;
        for (Long key : keys) {
            sum += map.get(key);
        }
        for (Long key : keys) {
            map.put(key, map.get(key)/sum);
        }
    }
}

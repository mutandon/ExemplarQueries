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
import java.util.Map;

/**
 * The weighted shortest path returns 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class WeightedShortestPath extends ShortestPath {
    @AlgorithmInput
    protected Map<Long,Double> edgeWeights; 
    @AlgorithmInput
    protected double minWeight; 
    
    public WeightedShortestPath() {
        minWeight = 1; 
    }
    
    @Override
    protected double distance(Long label, Long adjNode) {
        Double weight = edgeWeights.get(label);
        if (weight != null) {
            return weight; 
        }
        return minWeight;
    }
    
    public void setEdgeWeights(Map<Long, Double> edgeWeights) {
        this.edgeWeights = edgeWeights;
    }

    public void setMinWeight(double minWeight) {
        this.minWeight = minWeight;
    }
}

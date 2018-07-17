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
package eu.unitn.disi.db.exemplar.core.ranking;

import eu.unitn.disi.db.command.exceptions.AlgorithmExecutionException;
import eu.unitn.disi.db.exemplar.core.VectorSimilarities;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Structural similarity between nodes as explained in [1]
 *
 * [1] Davide Mottin, Matteo Lissandrini, Themis Palpanas, Yannis Velegrakis.
 * Exemplar Queries: A New Way of Searching. VLDB Journal, 2016.
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class StructuralSimilarityScoring extends NodeSimilarityScoring {

    public StructuralSimilarityScoring() {
        aggFun = AggregationFunction.SUM;
    }

    @Override
    protected void algorithm() throws AlgorithmExecutionException {

    }

    /**
     *
     * @param node1
     * @param node2
     * @return
     */
    @Override
    protected double score(BitSet node1, BitSet node2) {
        assert node1.size() == node2.size();
        Map<Long, Double> vector1 = new HashMap<>(), vector2 = new HashMap<>();        
        return VectorSimilarities.cosine(vector1, vector2, true);
    }

    /**
     * See Equation 5 in the paper
     *
     * @param vector
     * @param levelNodes
     * @param level
     */
    protected void updateVector(Map<Long, Double> vector, Map<Long, Integer> levelNodes, int level) {
        Set<Long> labels = levelNodes.keySet();
        Double value;
        int sqLevel = level * level;

        for (Long label : labels) {
            value = vector.get(label);
            if (value == null) {
                value = 0.0;
            }
            value += (levelNodes.get(label) * labelInformativeness.get(label)) / sqLevel;

            vector.put(label, value);
        }
    }

    @Override
    public Map<Long, Double> getNodesScoring(Collection<Long> nodesToScore) {
        timer.reset();
        timer.start();

        Map<Long, Double> nodeScoring = new ConcurrentHashMap<>((nodesToScore.size() * 4 / 3));

        nodesToScore.parallelStream().forEach(toRank -> {
            double similarity = 0.0;
            double value;
            BitSet node1;
            BitSet node2 = graphTables.get(toRank);
            for (Long queryNode : queryNodes) {
                node1 = graphTables.get(queryNode);
                if (node1 == null) {
                    throw new NullPointerException("Query tables for node " + queryNode + " are NULL");
                }
                value = score(node1, node2);
                switch (aggFun) {
                    case SUM:
                        similarity += value;
                        break;
                    case MAX:
                        similarity = Math.max(value, similarity);
                        break;
                }
            }
            nodeScoring.put(toRank, similarity / (aggFun == AggregationFunction.AVG ? queryNodes.size() : 1));
        });
        timer.stop();
        return nodeScoring;
    }

    @Override
    public Double getNodeScoring(Long nodeToScore) {
        double similarity = 0.0;
        double value;
        BitSet node1;
        BitSet node2 = graphTables.get(nodeToScore);
        for (Long queryNode : queryNodes) {
            node1 = graphTables.get(queryNode);
            if (node1 == null) {
                throw new NullPointerException("Query tables for node " + queryNode + " are NULL");
            }
            value = score(node1, node2);
            switch (aggFun) {
                case SUM:
                    similarity += value;
                    break;
                case MAX:
                    similarity = Math.max(value, similarity);
                    break;
            }
        }
        return similarity / (aggFun == AggregationFunction.AVG ? queryNodes.size() : 1);
    }

    
    @Override
    public Double getCumulativeScore(Collection<Long> nodesToScore) {
        return nodesToScore.stream().map(toRank -> {  
            return getNodeScoring(toRank);
        }).reduce((a,b)->a+b).get();     
    }
}

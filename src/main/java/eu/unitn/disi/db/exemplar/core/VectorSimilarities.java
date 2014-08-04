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

package eu.unitn.disi.db.exemplar.core;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class VectorSimilarities {
    
    private VectorSimilarities() {
    }
    
    public static double cosine(Map<Long, Double> vector1, Map<Long,Double> vector2, boolean normalize)
    {
        if (vector1 == null || vector2 == null) {
            return 0.0;
        }
        if (normalize) {
            normalize(vector1);
            normalize(vector2);
        }
        //We use cosine as a metric
        Set<Long> labels = vector1.keySet();
        double intersection = 0, v1SqNorm = 0, v2SqNorm = 0, value;
        for (Long l : labels) {
            value = vector1.get(l);
            v1SqNorm += value * value;
            if (vector2.containsKey(l)) {
                intersection += value * vector2.get(l);
            }
        }
        labels = vector2.keySet();
        for (Long l : labels) {
            value = vector2.get(l);
            v2SqNorm += value * value;
        }

        return intersection / (Math.sqrt(v1SqNorm * v2SqNorm)/* - intersection*/);
    }
    
    public static void normalize(Map<Long, Double> vector) {
        Set<Long> keys = vector.keySet();
        double sum = 0;
        for (Long key : keys) {
            sum += vector.get(key) * vector.get(key);
        }
        sum = Math.sqrt(sum);
        for (Long key : keys) {
            vector.put(key, vector.get(key)/sum);
        }
    }
}

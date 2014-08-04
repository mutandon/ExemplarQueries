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

import eu.unitn.disi.db.grava.graphs.Edge;
import java.util.Collection;

/**
 * Represents a weighted Path
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class WeightedPath implements Comparable<WeightedPath> {
    private double weight; 
    private Collection<Edge> path; 

    public WeightedPath() {
        this(0.0, null);
    }
    
    public WeightedPath(double weight, Collection<Edge> path) { 
        this.weight = weight;
        this.path = path;
    }
    
    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Collection<Edge> getPath() {
        return path;
    }

    public void setPath(Collection<Edge> path) {
        this.path = path;
    }

    public void merge(WeightedPath path) {
        this.path.addAll(path.path);
        this.weight += path.weight;
    }
    
    @Override
    public String toString() {
        return "(" + weight + ", " + path + ')';
    }

    @Override
    public int compareTo(WeightedPath o) {
        if (o == null || weight > o.weight)
            return 1;
        if (weight < o.weight)
            return -1; 
        return 0; 
    }
    
}

/*
 * The MIT License
 *
 * Copyright 2015 Matteo Lissandrini <ml@disi.unitn.eu>.
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
package eu.unitn.disi.db.exemplar.core.algorithms.steps;

import eu.unitn.disi.db.command.util.LoggableObject;
import eu.unitn.disi.db.command.util.StopWatch;
import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;


public abstract class AlgorithmStep<T  extends RelatedQuery> extends LoggableObject implements Callable<List<T>>{
    public static final int MAX_RELATED = 100000; // Warn or break after more than 50K results
    public static final int WARN_TIME = 30000;  // Warn if computation took more than 30 seconds

    protected final Iterator<Long> graphNodes;
    protected final Multigraph query;
    protected final Multigraph graph;
    protected final int threadNumber;
    protected final StopWatch watch;
    protected final boolean limitComputation;
    protected final boolean skipSave;

    public AlgorithmStep(int threadNumber, Iterator<Long> kbConcepts, Multigraph query, Multigraph targetSubgraph, boolean limitComputation, boolean skipSave) {
        this.threadNumber = threadNumber;
        this.graphNodes = kbConcepts;
        this.query = query;
        this.graph = targetSubgraph;
        this.limitComputation = limitComputation;
        this.skipSave = skipSave;

        this.watch = new StopWatch(StopWatch.TimeType.CPU);

    }


    @Override
    public abstract List<T> call() throws Exception;

}

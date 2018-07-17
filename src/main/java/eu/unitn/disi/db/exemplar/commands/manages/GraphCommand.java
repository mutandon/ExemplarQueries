/*
 * The MIT License
 *
 * Copyright 2016 Kuzeko.
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
package eu.unitn.disi.db.exemplar.commands.manages;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.DynamicInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.Command;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class GraphCommand extends Command {

    protected String outDir;
    protected String kbPath;
    protected String labelFrequenciesFile;
    protected String hubsFile;
    protected int threads;
    
    protected BigMultigraph graph;
    protected Map<Long, Integer> labelFrequencies;
    //protected List<Long> sortedLabels;
    protected Set<Long> skipNodes = new HashSet<>();
    protected StopWatch watch = new StopWatch();

    @Override
    protected void execute() throws ExecutionException {

    }

    protected void prepareData() throws IOException, NullPointerException, InvalidClassException, ParseException {
        labelFrequencies = new HashMap<>(6500);
        
        info("Loading label frequencies from %s", labelFrequenciesFile);
        CollectionUtilities.readFileIntoMap(labelFrequenciesFile, " ", labelFrequencies, Long.class, Integer.class);
        info("Loaded label frequencies in %s ms", watch.getElapsedTimeMillis());

        
        info("Loading the Graph");
        if (graph == null) {
            graph = new BigMultigraph(this.kbPath + "-sin.graph", this.kbPath + "-sout.graph"/*, knowledgebaseFileSize*/);
            info("Loaded graph from file-path %s", this.kbPath);
        }
        if (graph == null) {
            throw new IllegalStateException("Null Knowledgebase!!");
        }
        info("Graph loaded into %s ms", watch.getElapsedTimeMillis());

        watch.reset();
        info("1. Computing label set");
        graph.labelSet();
        info("Computed label set in %s ms", watch.getElapsedTimeMillis());
        
//        info("2. Computing and sorting label set");
//        sortedLabels = new ArrayList<>(labelFrequencies.keySet());
//        Collections.sort(sortedLabels, (Long o1, Long o2) -> labelFrequencies.get(o1).compareTo(labelFrequencies.get(o2)));       
//        watch.reset();

        
        if (hubsFile != null && !hubsFile.isEmpty()) {
            CollectionUtilities.readFileIntoCollection(hubsFile, skipNodes, Long.class);
        }
    }

    @Override
    protected String commandDescription() {
        return "empty command";
    }
    
    @CommandInput(
            consoleFormat = "--threads",
            defaultValue = "4",
            mandatory = false,
            description = "Threads in parallel to actually run tables << smaller than chunks")
    public void setNumThreads(int cores) {
        this.threads = cores;
    }

    
    @DynamicInput(
            consoleFormat = "--graph",
            description = "multigraph used as a knowledge-base")
    public void setGraph(BigMultigraph graph) {
        this.graph = graph;
    }

    @CommandInput(
            consoleFormat = "-kb",
            defaultValue = "",
            description = "path to the knowledgbase sin and sout files, just up to the prefix, like InputData/freebase ",
            mandatory = true)
    public void setKbPath(String kb) {
        this.kbPath = kb;
    }

    @CommandInput(
            consoleFormat = "-lf",
            defaultValue = "",
            mandatory = true,
            description = "label frequency file formatted as 'labelid frequency'")
    public void setLabelFrequencies(String labelFrequencies) {
        this.labelFrequenciesFile = labelFrequencies;
    }

    @CommandInput(
            consoleFormat = "-dir",
            defaultValue = "/tmp/tables",
            description = "The directory where to store tables",
            mandatory = false)
    public void setQueriesOut(String dir) {
        this.outDir = dir;
    }

    @CommandInput(
            consoleFormat = "-h",
            defaultValue = "",
            mandatory = false,
            description = "node big hubs file")
    public void setHubs(String hubs) {
        this.hubsFile = hubs;
    }

}

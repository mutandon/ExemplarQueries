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
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.exemplar.core.storage.SetIndex;
import eu.unitn.disi.db.exemplar.core.storage.StorableTable;
import eu.unitn.disi.db.grava.graphs.Edge;
import eu.unitn.disi.db.mutilities.Pair;
import eu.unitn.disi.db.mutilities.StopWatch;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Kuzeko
 */
public class ComputeBitsetLvl1 extends GraphCommand {

    private boolean runTest = false;

    @Override
    @SuppressWarnings("unchecked")
    protected void execute() throws ExecutionException {

        try {

            File savingDir = new File(this.outDir);

            // First, make sure the path exists
            // This will tell you if it is a directory
            if (!savingDir.exists() || !savingDir.isDirectory() || !savingDir.canWrite()) {
                String cause = "Uknown";

                if (!savingDir.exists()) {
                    cause = "Not found";
                } else if (!savingDir.isDirectory()) {
                    cause = "Is not a Directory";
                } else if (!savingDir.canWrite()) {
                    cause = "Is not Writable";
                }

                throw new IOException("Illegal directory path: '" + this.outDir + "'  " + cause);
            }

            watch.start();
            this.prepareData();

            ArrayList<Set<Integer>> bitsets;
            bitsets = new ArrayList<>();
            HashMap<Long, Integer> nodesHahMap;

            if (!runTest) {
                info("STARTING COMPUTATION of " + this.getClass().getCanonicalName());

                info("Building list of nodes");
                watch.reset();
                long[] graphNodes = new long[graph.numberOfNodes()];
                int[] graphClusters = new int[graphNodes.length];
                int idx = 0;
                for (long node : graph) {
                    graphNodes[idx] = node;
                    idx++;
                }
                watch.stop();
                info("List of %s nodes build in %s ms", graphNodes.length, watch.getElapsedTimeMillis());
                watch.reset();
                List<Long> labels = new ArrayList<>(labelFrequencies.keySet());
                Collections.sort(labels);
                HashMap<Long, Integer> positions = new HashMap<>(labels.size() * 4 / 3);
                idx = 0;
                for (Long l : labels) {
                    positions.put(l, idx);
                    idx++;
                }
                int numLabels = labels.size();
                info("List of %s labels build in %s ms", labels.size(), watch.getElapsedTimeMillis());

                HashMap<Set<Integer>, Integer> bits = new HashMap<>(graphNodes.length / 100);
                HashSet<Integer> bb;
                Iterator<Edge> it;

                long edges = 0;
                long lastLb;
                long lbl;
                int numLargeNodes = 0;
                int clusterId = 0;
                int nodeIndex = 0;
                bb = new HashSet<>(numLabels/10); // This will be re-initialized at the end of the loop
                for (long node : graphNodes) {
                    if (graph.degreeOf(node) > 100_000) {
                        info("Processing large node with %s edges", graph.degreeOf(node));
                        numLargeNodes++;
                    }

                    it = graph.outgoingEdgesIteratorOf(node);
                    lastLb = -1;
                    while (it.hasNext()) {
                        lbl = it.next().getLabel();
                        if (lbl != lastLb) {
                            bb.add(positions.get(lbl));
                            lastLb = lbl;
                        }
                        edges++;
                    }
                    it = graph.incomingEdgesIteratorOf(node);
                    lastLb = -1;
                    while (it.hasNext()) {
                        lbl = it.next().getLabel();
                        if (lbl != lastLb) {
                            bb.add(numLabels + positions.get(lbl));
                            lastLb = lbl;
                        }
                        edges++;
                    }
                    Integer bx = bits.get(bb);
                    if (bx == null) {
                        bits.put(bb, clusterId);
                        bx = clusterId;
                        clusterId++;
                        bb = new HashSet<>(numLabels/10);
                    } else {
                        bb.clear();
                    }
                    graphClusters[nodeIndex] = bx;
                    nodeIndex++;
                    if (nodeIndex > 0 && nodeIndex % 5_000_000 == 0) {
                        info("Processed %s nodes (%s large) and %s edges, current size %s", nodeIndex, numLargeNodes, edges, bits.size());
                    }
                }

                bitsets = new ArrayList<>(bits.size());
                for (int i = 0; i < bits.size(); i++) {
                    bitsets.add(null);                    
                }
                for (Entry<Set<Integer>, Integer> en : bits.entrySet()) {
                    bitsets.set(en.getValue(),en.getKey());
                }

                info("Writing   %s maps  to file  %s", bits.size(), this.outDir + File.separator + "bitsets-l1.array");
                FileOutputStream fos = new FileOutputStream(this.outDir + File.separator + "bitsets-l1.array");
                try (ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                    oos.writeObject(bitsets);
                }

                info("Writing  the map as StorableTable");

                fos = new FileOutputStream(this.outDir + File.separator + "node-hashing-id-l1.tbl");
                try (ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                    oos.writeObject(graphNodes);
                }

                fos = new FileOutputStream(this.outDir + File.separator + "node-hashing-values-l1.tbl");
                try (ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                    oos.writeObject(graphClusters);
                }
            } else {
                info("Reading maps from file  %s", this.outDir + File.separator + "bitsets-l1.array");
                watch.reset();
                watch.start();
                FileInputStream fis = new FileInputStream(this.outDir + File.separator + "bitsets-l1.array");
                try (ObjectInputStream iis = new ObjectInputStream(fis)) {
                    bitsets = (ArrayList<Set<Integer>>) iis.readObject();
                } catch (ClassNotFoundException ex) {
                    Logger.getLogger(ComputeBitsetLvl1.class.getName()).log(Level.SEVERE, null, ex);
                }
                watch.stop();
                info("Loaded %s bitsets in %s ms", bitsets.size(), watch.getElapsedTimeMillis());

                info("Reading  maps from file  %s", this.outDir + File.separator + "node-hashing-*-l1");
                watch.reset();
                watch.start();
                StorableTable stp = new StorableTable(this.outDir, "node-hashing-id-l1", "node-hashing-values-l1", this.graph.numberOfNodes() * 5 / 4);
                stp.load();
                nodesHahMap = new HashMap<>(stp.getNodes().size() * 4 / 3);
                HashSet<Integer> diff = new HashSet<>(bitsets.size() * 4 / 3);
                for (Pair<Long, Integer> p : stp) {
                    diff.add(p.getSecond());
                    nodesHahMap.put(p.getFirst(), p.getSecond());
                }
                watch.stop();
                info("Loaded  map for %s (%s) in %s ms", nodesHahMap.size(), diff.size(), watch.getElapsedTimeMillis());

                info("Test performance");
                Random rand = new Random();
                StopWatch st = new StopWatch();
                
                int tmpMatches = 0;                
                int NUM_TESTS = 2000;
                int size = 0;

                List<Long> times = new ArrayList<>(NUM_TESTS+2);
                List<Integer> matches = new ArrayList<>(NUM_TESTS+2);
                Long cumulativeTime = 0l;
                int cumulativeMatches =0;
                
                st.start();
                final int KEY=13; //23
                SetIndex stIndx = new SetIndex(bitsets, KEY);//23
                st.stop();
                info("Created index with key %s of size %s in %s ms:  Stats ", KEY, stIndx.getIndexSize(), st.getElapsedTimeMillis());
                int[] stats = stIndx.getIndexNodeSizeStats();
                info("Min %s \t Median %s \t 90th %s \t Max %s", stats[0], stats[1],stats[2],stats[3]);
                
                st.reset();
                for (int i = 0; i < NUM_TESTS; i++) {
                    Set<Integer> bmap = bitsets.get(rand.nextInt(bitsets.size()));
                    size += bmap.size();
                    st.start();
                    Iterator<Pair<Integer, Set<Integer>>> cands = stIndx.getCandidateSuperSets(bmap);
                    if(!cands.hasNext()){
                        throw new IllegalStateException("No candidates! This doesn't compute");
                    }
                    while(cands.hasNext()){
                        Set<Integer> target = cands.next().getSecond();
                        //temp.clear();
                        //temp.or(bmap);
                        //temp.andNot(target);
                        if (target.containsAll(bmap)) {
                            tmpMatches++;
                        }
                    }
                    st.stop();
                    times.add(st.getElapsedTimeMillis());
                    matches.add(tmpMatches);
                    cumulativeTime +=st.getElapsedTimeMillis();
                    cumulativeMatches += tmpMatches;
                    st.reset();
                    tmpMatches =0;
                }
                
                Collections.sort(times);
                Collections.sort(matches);
                                                
                info("Run %s comparison found %s matches in %s ms. Avg bitset size %s", NUM_TESTS, cumulativeMatches, cumulativeTime, size / NUM_TESTS);
                info("AVG matches %s \t   AVG time %s", cumulativeMatches/NUM_TESTS, cumulativeTime/NUM_TESTS );
                int medianIdx = NUM_TESTS/2;
                info("Median matches %s \t  Median time %s", matches.get(medianIdx), times.get(medianIdx));
                medianIdx = 9*NUM_TESTS/10;
                info("90Perc matches %s \t  90Perc time %s", matches.get(medianIdx), times.get(medianIdx));
            }

        } catch (ParseException ex) {
            fatal("Query parsing failed", ex);
        } catch (IOException ex) {
            Logger.getLogger(ComputeBitsetLvl1.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @CommandInput(
            consoleFormat = "--test",
            defaultValue = "false",
            mandatory = false,
            description = "labels to test file")
    public void setLabelsFile(boolean tst) {
        this.runTest = tst;
    }

    @Override
    protected String commandDescription() {
        return "compute all the tables in  -dir";
    }

}

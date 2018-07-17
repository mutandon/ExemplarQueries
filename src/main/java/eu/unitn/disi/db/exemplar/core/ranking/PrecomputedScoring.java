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
import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ranking function that is based on a node-weight map
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class PrecomputedScoring extends NodeScoring {

    private Map<Long, Double> weights;
    private String separator = " ";
    private Double defaultWeight = 0.0;
    
   /**
    * 
    * @param weightFile file to read, should be a map of LONG and Double values
    * @param normalize shift values in the [0-1]
    * @param separator node ids and weights are separated by
    */
    public PrecomputedScoring(Path weightFile, boolean normalize, String separator ) {
        this.separator = separator;
        this.weights  = new HashMap<>(10_000_000);
        try {
            CollectionUtilities.readFileIntoMap(weightFile.toString(), this.separator, weights, Long.class, Double.class);
            if(normalize){
                CollectionUtilities.normalizeMap(weights);
            }
        } catch (IOException | NullPointerException | ParseException ex) {
            Logger.getLogger(PrecomputedScoring.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    public PrecomputedScoring(Path weightFile) {
        this(weightFile,true, "\t");
    }

    public PrecomputedScoring(Path weightFile, String sep) {
        this(weightFile,true, sep);
    }
    
    public PrecomputedScoring(String weightFile, String sep) {
           this(Paths.get(weightFile),true, sep);
    }
    
    public PrecomputedScoring(Map<Long, Double> weights){
        this.weights = weights;
    }

    
    @Override
    protected void algorithm() throws AlgorithmExecutionException {
    }

    

    @Override
    public Map<Long, Double> getNodesScoring(Collection<Long> nodesToScore) {
        timer.reset();
        timer.start();

        Map<Long, Double> nodeScoring = new HashMap<>((nodesToScore.size() * 4 / 3));

        nodesToScore.forEach(toRank -> {
            nodeScoring.put(toRank, weights.getOrDefault(toRank, defaultWeight));
        });
        timer.stop();
        return nodeScoring;

    }

    @Override
    public Double getNodeScoring(Long nodeToScore) {
        return weights.getOrDefault(nodeToScore, defaultWeight);
    }
    
    @Override
    public Double getCumulativeScore(Collection<Long> nodesToScore) {
        return nodesToScore.stream().map(toRank -> {  
            return weights.getOrDefault(toRank, defaultWeight);
        }).reduce((a,b)->a+b).get();     
    }

    
}

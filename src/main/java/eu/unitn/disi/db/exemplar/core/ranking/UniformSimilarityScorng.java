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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A ranking function that gives the same weight to all the nodes.
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class UniformSimilarityScorng extends NodeScoring {

    @Override
    protected void algorithm() throws AlgorithmExecutionException {
    }

    
    
    @Override
    public Map<Long, Double> getNodesScoring(Collection<Long> nodesToScore) {
        timer.reset();
        timer.start();

        Map<Long, Double> nodeScoring = new ConcurrentHashMap<>((nodesToScore.size() * 4 / 3));

        nodesToScore.parallelStream().forEach(toRank -> {
            nodeScoring.put(toRank, 1.0);
        });
        timer.stop();
        return new HashMap<>(nodeScoring);

    }

    @Override
    public Double getNodeScoring(Long nodeToScore) {
       return 1.0;
    }
    
    @Override
    public Double getCumulativeScore(Collection<Long> nodesToScore) {
        return nodesToScore.stream().map(toRank -> {  
            return getNodeScoring(toRank);
        }).reduce((a,b)->a+b).get();     
    }
}

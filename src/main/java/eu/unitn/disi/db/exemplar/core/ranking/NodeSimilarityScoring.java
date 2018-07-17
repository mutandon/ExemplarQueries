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


import eu.unitn.disi.db.command.algorithmic.AlgorithmInput;
import java.util.BitSet;
import java.util.Map;


/**
 * Abstract class computing the similarity in a set of nodes. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public abstract class NodeSimilarityScoring extends NodeScoring {
    
    @AlgorithmInput
    protected Map<Long, BitSet> graphTables;
    
    
    
    protected abstract double score(BitSet node1, BitSet node2);
    
    public void setGraphTables(Map<Long, BitSet> graphTables) {
        this.graphTables = graphTables;
    }

        
}

/*
 * The MIT License
 *
 * Copyright 2018 Matteo Lissandrini <ml@disi.unitn.eu>.
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
package eu.unitn.disi.db.exemplar.core.storage;

import eu.unitn.disi.db.mutilities.Pair;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class SetIndex {

    private final ArrayList<Set<Integer>> bitsets;
    
    private final Map<BitSet, Set<Integer>> index;
    
    private final int keySize;
    
 
    
 
    
    /**
     * 
     * @param bitsets to index
     * @param keySize size of the signature
     */    
    public SetIndex(ArrayList<Set<Integer>> bitsets, int keySize) {
        this.keySize=keySize;
        this.bitsets = bitsets;
        this.index = new HashMap<>((bitsets.size())/(keySize));
        int bitsetPos =0;        
        for(Set<Integer> bs : bitsets){
            
            BitSet sign = getSignature(bs);
            Set<Integer> indexNode = index.get(sign);
            if(indexNode==null){
                indexNode = new HashSet<>();                
                index.put(sign, indexNode);
            }
            indexNode.add(bitsetPos);        
            bitsetPos++;
        }
        
        
        
    }
    
    private BitSet getSignature(Set<Integer> target){
        BitSet sign = new BitSet(this.keySize);
        for(int id : target){
            sign.set(id%this.keySize);
        }
        
        return sign;
    }
    
    public Iterator<Pair<Integer, Set<Integer>>> getCandidateSuperSets(Set<Integer> bs){
        BitSet sign = getSignature(bs);
        Set<Integer> matches = new HashSet<>(30);
        BitSet tmp = new BitSet(keySize);
        for(Entry<BitSet, Set<Integer>> entry :  this.index.entrySet() ){
            if(isSubset(tmp,sign,entry.getKey())){
               matches.addAll(entry.getValue()); 
            }            
        }
        Iterator<Pair<Integer, Set<Integer>>> it = new IndexIterator(matches);
        return it;
    }
    
    /**
     *
     * @param tmp
     * @param a
     * @param b
     */
    private static boolean isSubset(BitSet tmp, BitSet contained, BitSet container) {
        tmp.clear();
        tmp.or(contained);
        tmp.andNot(container);
        return tmp.isEmpty();
    }

    
    
    
    /**
     * Iterates over the bitsets given the indexes
     */
    private class IndexIterator implements Iterator<Pair<Integer,Set<Integer>>> {
        private final Iterator<Integer> posIter;
        protected IndexIterator(Set<Integer> indxs){
            this.posIter = indxs.iterator();
        }
        
        @Override
        public boolean hasNext() {
            return this.posIter.hasNext();
        }

        @Override
        public Pair<Integer,Set<Integer>> next() {
            int idx = this.posIter.next();
            return new Pair<>(idx,bitsets.get(idx));
        }
        
    }
    
    /**
     * 
     * @return the size of the index pointers
     */
    public int getIndexSize(){
        return this.index.size();
    }
    
    
    /**
     * 
     * @return Min, median, 90th perc, Max sizes
     */
    public int[] getIndexNodeSizeStats(){
        ArrayList<Integer> sizes = new ArrayList<>(this.index.size());
        for(Set<Integer> s : this.index.values()){
            sizes.add(s.size());
        }
        Collections.sort(sizes);
        int nN =sizes.size();
        return new int[] {sizes.get(0), 
                    sizes.get(nN/2), 
                    sizes.get(nN*9/10), 
                    sizes.get(nN-1), 
                 };
    }
    
}

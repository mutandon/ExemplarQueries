/*
 * Copyright (C) 2016 Matteo Lissandrini
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
package eu.unitn.disi.db.exemplar.core.storage;

import eu.unitn.disi.db.mutilities.data.CollectionUtilities;
import eu.unitn.disi.db.mutilities.LoggableObject;
import eu.unitn.disi.db.mutilities.Pair;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author Matteo Lissandrini
 */
public class StorableTriple extends LoggableObject implements Iterable<Pair<Pair<Long, Long>, Integer>> {

    private static final int INITIAL_SIZE = 1000;

    
    private final String pairsPath;
    
    private final String cadyPath;       
    private final int initialSize;


    /**
     * Nodes IDS
     */
    private ArrayList<Pair<Long,Long>> pairs;

    /**
     * Nodes Cardinality
     */
    private ArrayList<Integer> cady;

    
    
    /**
     *
     * @param dirPath directory where to store the table
     * @param initialSize
     * @param nameKeyFile
     * @param nameValueFile
     * @param readOnly
     * @throws java.io.IOException
     */
    public StorableTriple(String dirPath,  String nameKeyFile, String nameValueFile, int initialSize, boolean readOnly) throws IOException {
        
        
        this.initialSize = initialSize;
        this.pairsPath = dirPath.concat(File.separator).concat(nameKeyFile+".obj");       
        this.cadyPath = dirPath.concat(File.separator).concat(nameValueFile+".tbl");

        File savingDir = new File(dirPath);
        
        // First, make sure the path exists
        // This will tell you if it is a directory
        if (!savingDir.exists() || !savingDir.isDirectory() || (!readOnly && !savingDir.canWrite())) {
            String cause = "Uknown";
            
            if(!savingDir.exists()) {
                cause = "Not found";
            } else if ( !savingDir.isDirectory()){
                cause = "Is not a Directory";            
            } else if ( !savingDir.canWrite() ) {
                cause = "Is not Writable";                
            }
            
            throw new IOException("Illegal directory path: '"+dirPath + "'  "+cause);
        }
        this.pairs = new ArrayList<>(initialSize);
        this.cady = new ArrayList<>(initialSize);

    }
    
    /**
     *
     * @param dirPath directory where to store the table
     * @throws java.io.IOException
     */
    public StorableTriple(String dirPath) throws IOException {
        this(dirPath, "pairs-list", "cardinalities" , INITIAL_SIZE, false);

    }

    /**
     * append node and cardinality,  does not overwrite
     * @param node1
     * @param node2
     * @param cd
     * @return current number of elements
     */
    public int put(long node1, long node2, int cd) {
        pairs.add(new Pair<>(node1, node2));
        cady.add(cd);
        return pairs.size();
    }
    
    
    public int put(Pair<Long, Long> p, int cd) {
        pairs.add(p);
        cady.add(cd);
        return pairs.size();
    }
    
    
    public int putAll(Map<Pair<Long, Long>, Integer> table) {
        for (Entry<Pair<Long, Long>, Integer> entry : table.entrySet()) {
            pairs.add(entry.getKey());
            cady.add(entry.getValue());
        }
        return pairs.size();
    }

    
    /**
     * 
     * @return  the map
     */
    public Map<Pair<Long,Long>, Integer> getNodesMap(){
        Map<Pair<Long, Long>, Integer> map = new HashMap<>();
        for(Pair<Pair<Long, Long>, Integer> p : this){
            map.put(p.getFirst(), p.getSecond());
        }
        return map;
    }

    /**
     * The nodes in this table
     * @return 
     */
    public List<Pair<Long, Long>> getNodes(){
        return this.pairs;
    }
    
    
    /**
     * 
     * @return true if the current table is stored on disk
     */
    public boolean isStored(){
        
            File pairFile = new File(this.pairsPath);            
            File cadyFile = new File(this.cadyPath);
            boolean pe =  pairFile.exists();            
            boolean cf = cadyFile.exists();
            if((pe ^ cf)){
                throw new IllegalStateException("Pairs and Cardinality files are corrupt");
            }
            return pairFile.exists();
        
        
    }
    
    /**
     * Saves the list of pairs as serialized object  and the cardinalities as array of int
     *
     * @throws java.io.IOException
     */
    public void save() throws IOException {
        try (
                OutputStream file = new FileOutputStream(this.pairsPath);
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutput output = new ObjectOutputStream(buffer);) {
            
            output.writeObject(this.pairs);

        } catch (IOException ex) {
            throw new IOException("Could Not Serialize Pairs File", ex);
        }
        try (
                OutputStream file = new FileOutputStream(this.cadyPath);
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutput output = new ObjectOutputStream(buffer);) {

            int[] toOut = CollectionUtilities.convertListIntegers(this.cady);
            output.writeObject(toOut);

        } catch (IOException ex) {
            throw new IOException("Could Not Serialize Cardinalities File", ex);
        }
        debug("Saved on %s", this.pairsPath);
    }

    public void clear(){
        this.pairs.clear();
        this.cady.clear();
    }
    
    /**
     * Loads the list of nodes from two files containing the two arrays
     *
     * @return true if everything went well
     * @throws java.io.IOException
     */
    @SuppressWarnings("unchecked")
    public boolean load() throws IOException {
        this.pairs = new ArrayList<>(this.initialSize);
        this.cady = new ArrayList<>(this.initialSize);
                    
        if(!this.isStored()){
            return false;
        }
        try (
                InputStream file = new FileInputStream(this.pairsPath);
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);
            ) {

            this.pairs = (ArrayList<Pair<Long,Long>>) input.readObject();
            
            

        } catch (ClassNotFoundException ex) {
            error("How could you let this happen?");
            return false;
        }
        try (
                InputStream file = new FileInputStream(this.cadyPath);
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);) {

            int[] rec = (int[]) input.readObject();
            for (int n : rec) {
                this.cady.add(n);
            }

        } catch (ClassNotFoundException ex) {
            error("How could you let this happen?");
            return false;
        }

        return true;
    }

    @Override
    public Iterator<Pair<Pair<Long, Long>, Integer>> iterator() {
        return new PairCardinalityIterator(pairs.iterator(), cady.iterator());
    }

    
    private class PairCardinalityIterator implements Iterator<Pair<Pair<Long, Long>, Integer>> {

        private final Iterator<Pair<Long, Long>> pairs;
        private final Iterator<Integer> cadys;

        public PairCardinalityIterator(Iterator<Pair<Long, Long>> nodes, Iterator<Integer> cadys) {
            this.pairs = nodes;
            this.cadys = cadys;
        }

        @Override
        public boolean hasNext() {
            if (pairs.hasNext() ^ cadys.hasNext()) {
                throw new IllegalStateException("Nodes iterator and cardinality iterator differ in status");
            }
            return pairs.hasNext();
        }

        @Override
        public Pair<Pair<Long, Long>, Integer> next() {
            return new Pair<>(this.pairs.next(), this.cadys.next());
        }

    }

}

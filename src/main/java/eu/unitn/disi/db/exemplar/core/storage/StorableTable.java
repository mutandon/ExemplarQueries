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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author Matteo Lissandrini
 */
public class StorableTable extends LoggableObject implements Iterable<Pair<Long, Integer>> {

    private static final int INITIAL_SIZE = 1000;

    
    private final String nodesPath;
    private final String cadyPath;
    private final int initialSize;


    /**
     * Nodes IDS
     */
    private ArrayList<Long> nodes;

    /**
     * Nodes Cardinality
     */
    private ArrayList<Integer> cady;

    
    
    /**
     *
     * @param dirPath directory where to store the table
     * @param nameKeyFile
     * @param nameValueFile
     * @param initialSize
     * @throws java.io.IOException
     */
    public StorableTable(String dirPath, String nameKeyFile, String nameValueFile, int initialSize) throws IOException {
        this.initialSize = initialSize;
        //this.label + "-" + this.depth + "-nodes
        this.nodesPath = dirPath.concat(File.separator).concat(nameKeyFile+".tbl");
        //this.label + "-" + this.depth + "-cardinalities
        this.cadyPath = dirPath.concat(File.separator).concat(nameValueFile+".tbl");

        File savingDir = new File(dirPath);
        //debug("StorableTable Base DIR %s", savingDir.getAbsolutePath());
        
        // First, make sure the path exists
        // This will tell you if it is a directory
        if (!savingDir.exists() || !savingDir.isDirectory() || !savingDir.canWrite()) {
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
        this.nodes = new ArrayList<>(initialSize);
        this.cady = new ArrayList<>(initialSize);

    }
    
    /**
     *
     * @param dirPath directory where to store the table
     * @param keyFile
     * @param valueFile
     * @throws java.io.IOException
     */
    public StorableTable(String dirPath, String keyFile, String valueFile) throws IOException {
        this(dirPath, keyFile, valueFile, INITIAL_SIZE);
    }

    /**
     * append node and cardinality,  does not overwrite
     * @param node
     * @param cd
     * @return current number of elements
     */
    public int put(long node, int cd) {
        nodes.add(node);
        cady.add(cd);
        return nodes.size();
    }
    
    
    public int putAll(Map<Long, Integer> table) {
        for (Entry<Long, Integer> pair : table.entrySet()) {
            nodes.add(pair.getKey());
            cady.add(pair.getValue());            
        }
        return nodes.size();
    }

    /**
     * 
     * @return number of elements
     */
    public int size(){
        return  this.nodes.size();
    }
    
    /**
     * 
     * @return  the map
     */
    public LinkedHashMap<Long, Integer> getNodesMap(){
        LinkedHashMap<Long, Integer> map = new LinkedHashMap<>(nodes.size()*4/3);
        for(Pair<Long, Integer> p : this){
            map.put(p.getFirst(), p.getSecond());
        }
        return map;
    }

    /**
     * The nodes in this table
     * @return 
     */
    public List<Long> getNodes(){
        return this.nodes;
    }
    
    
    /**
     * 
     * @return true if the current table is stored on disk
     */
    public boolean isStored(){
        
            File nodesFile = new File(this.nodesPath);
            File cadyFile = new File(this.cadyPath);
            if(cadyFile.exists() ^ nodesFile.exists()){
                throw new IllegalStateException("Node and Cardinality files are corrupt");
            }
            return nodesFile.exists();
        
        
    }
    
    /**
     * Saves the list of nodes as two arrays of long and int
     *
     * @throws java.io.IOException
     */
    public void save() throws IOException {
        try (
                OutputStream file = new FileOutputStream(this.nodesPath);
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutput output = new ObjectOutputStream(buffer);) {

            long[] toOut = CollectionUtilities.convertListLongs(this.nodes);
            output.writeObject(toOut);

        } catch (IOException ex) {
            throw new IOException("Could Not Serialize Table Nodes File", ex);
        }
        try (
                OutputStream file = new FileOutputStream(this.cadyPath);
                OutputStream buffer = new BufferedOutputStream(file);
                ObjectOutput output = new ObjectOutputStream(buffer);) {

            int[] toOut = CollectionUtilities.convertListIntegers(this.cady);
            output.writeObject(toOut);

        } catch (IOException ex) {
            throw new IOException("Could Not Serialize Table Cardinalities File", ex);
        }
        debug("Saved on %s", this.cadyPath);
    }

    /**
     * Loads the list of nodes from two files containing the two arrays
     *
     * @return true if everything went well
     * @throws java.io.IOException
     */
    public boolean load() throws IOException {
        this.nodes = new ArrayList<>(this.initialSize);
        this.cady = new ArrayList<>(this.initialSize);
                    
        if(!this.isStored()){
            return false;
        }
        try (
                InputStream file = new FileInputStream(this.nodesPath);
                InputStream buffer = new BufferedInputStream(file);
                ObjectInput input = new ObjectInputStream(buffer);
            ) {

            long[] rec = (long[]) input.readObject();
            
            for (long n : rec) {
                this.nodes.add(n);
            }

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
    public Iterator<Pair<Long, Integer>> iterator() {
        return new NodeCardinalityIterator(nodes.iterator(), cady.iterator());
    }

    public void clear() {
        this.nodes.clear();
        this.cady.clear();
    }

    
    private class NodeCardinalityIterator implements Iterator<Pair<Long, Integer>> {

        private final Iterator<Long> nodes;
        private final Iterator<Integer> cadys;

        public NodeCardinalityIterator(Iterator<Long> nodes, Iterator<Integer> cadys) {
            this.nodes = nodes;
            this.cadys = cadys;
        }

        @Override
        public boolean hasNext() {
            if (nodes.hasNext() ^ cadys.hasNext()) {
                throw new IllegalStateException("Nodes iterator and cardinality iterator differ in status");
            }
            return nodes.hasNext();
        }

        @Override
        public Pair<Long, Integer> next() {
            return new Pair<>(this.nodes.next(), this.cadys.next());
        }

    }

}

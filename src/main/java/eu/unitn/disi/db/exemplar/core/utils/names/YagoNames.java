/*
 * The MIT License
 *
 * Copyright 2017 Matteo Lissandrini <ml@disi.unitn.eu>.
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
package eu.unitn.disi.db.exemplar.utils.names;

import eu.unitn.disi.db.exemplar.utils.NamesProvider;
import eu.unitn.disi.db.mutilities.StringUtils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class YagoNames implements NamesProvider {

    private  HashMap<String, Long> nodeToId = null;
    private  HashMap<Long, String> idToNode = null;
    
    private  HashMap<String, Long> labelToId = null;
    private  HashMap<Long, String> idToLabel = null;

    private  String namesDirFilePath = null;

    public YagoNames(String namesDir) throws IOException {
        this.namesDirFilePath = namesDir;
        this.loadNames();

    }

    private void loadNames() throws IOException {
        nodeToId = new HashMap<>();
        idToNode = new HashMap<>();
        labelToId = new HashMap<>();
        idToLabel = new HashMap<>();
        
        fillMaps(Paths.get(namesDirFilePath, "yago-nodes-name.tsv").toString(), idToNode, nodeToId);
        fillMaps(Paths.get(namesDirFilePath, "yago-labels.tsv").toString(), idToLabel, labelToId);

    }

    private void fillMaps(String fileName, Map<Long, String> map, Map<String, Long> mapR) throws IOException {
        BufferedReader reader = null;
        long idx = 0l;
        try {
            String line;

            InputStream fileStream = new FileInputStream(fileName);

            reader = new BufferedReader(new InputStreamReader(fileStream));
            
            String[] splittedLine;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                idx++;
                try{
                    if (line.length() > 0) {
                        splittedLine = StringUtils.fastSplit(line, '\t', 2);
                        if(splittedLine == null || splittedLine[0]==null || splittedLine[1] == null){
                            throw new IOException("Labels File is not formatted correctly: NULL AT "+idx);
                        }
                        mapR.put(splittedLine[1], Long.parseLong(splittedLine[0]));
                        map.put(Long.parseLong(splittedLine[0]), splittedLine[1]);
                    }
                } catch (NumberFormatException ne){
                    throw new IOException("Labels File is not formatted correctly at line "+idx, ne);
                }
            }
        } catch (IOException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException("Labels File is not formatted correctly at line "+idx, ex);
        } finally {
            StringUtils.close(reader);
        }

    }
    
    
    @Override
    public String getName(){
        return "Yago";
    }

    @Override
    public String getNodeNameFromID(Long id) {
        return this.idToNode.get(id);
    }

    @Override
    public Long getNodeIDFromName(String name) {
        return this.nodeToId.get(name);
    }

    @Override
    public String getLabelNameFromID(Long id) {
        return this.idToLabel.get(id);
    }

    @Override
    public Long getLabelIDFromName(String name) {
        return this.labelToId.get(name);
    }

}

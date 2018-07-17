/*
 * Copyright (C) 2012 Davide Mottin <mottin@disi.unitn.eu>
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
package eu.unitn.disi.db.exemplar.utils.names;

import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import eu.unitn.disi.db.exemplar.utils.NamesProvider;
import eu.unitn.disi.db.mutilities.StringUtils;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Represents the different notations that are used in the Freebase data-dump files
 * They can be changed or embedded in a configuration file
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class FreebaseNames implements NamesProvider{

    public FreebaseNames() throws IOException {
        loadProperties();
    }

    public FreebaseNames(String propertiesFilePath) throws IOException {
        this.propertiesFilePath = propertiesFilePath;
        loadProperties();
    }
    
    
    @Override
    public String getName(){
        return "Freeabse";
    }
    
    @Override
    public String getNodeNameFromID(Long id) {
        return convertLongToMid(id);
    }

    @Override
    public Long getNodeIDFromName(String name) {
        return convertMidToLong(name);
    }

    @Override
    public String getLabelNameFromID(Long id) {
        try {            
            return getPropertyMid(id);
        } catch (IOException ex) {
               throw new RuntimeException("Could not map id "+ id, ex);
        }
    }

    @Override
    public Long getLabelIDFromName(String name) {
        
        try {
            return getPropertyId(name);
        } catch (IOException ex) {
            throw new RuntimeException("Could not map label "+ name, ex);
        }
        
    }
    
    
    
    /*
     * IMPORTANT NOTE
     * -----------------
     * Other types are never used/present in freebase datadump, but maybe they
     * will be used in the future
     *
     * The following is the list of not found namespaces, documented in the online
     * version
     *
     * /type/object/[id|guid|timestamp|creator|permission|attribution|search|mid]
     */
    public enum FreebaseBasicType {
        DOMAIN("/type/domain"),
        TOPIC("/type/type"),
        ENTITY("/common/topic"),
        PROPERTY("/type/property")
        ;

        private final String freebaseId;

        private FreebaseBasicType(String freebaseId) {
            this.freebaseId = freebaseId;
        }

        public String freebaseId() {
            return freebaseId;
        }
    }

    private  Map<String,Long> propertiesToId = null;
    private  Map<Long,String> idToProperties = null;
    private  String propertiesFilePath = null;

    /**
     * Represents the patterns that, once found, removes the line from the tsv file
     */
    public static final String SKIP_PATTERNS = ".*\\t/user.*|"
            + ".*\\t/freebase/(?!domain_category).*|"
            + ".*/usergroup/.*|"
            + ".*\\t/community/.*\\t.*|"
            + ".*\\t/type/object/type\\t.*|"
            + ".*\\t/type/domain/.*\\t.*|"
            + ".*\\t/type/property/(?!expected_type|reverse_property)\\b.*|"
            + ".*\\t/type/(user|content|attribution|extension|link|namespace|permission|reflect|em|karen|cfs|media).*|"
            + ".*\\t/common/(?!document|topic)\\b.*|"
            + ".*\\t/common/document/(?!source_uri)\\b.*|"
            + ".*\\t/common/topic/(description|image|webpage|properties|weblink|notable_for|article).*|"
            + ".*\\t/type/type/(?!domain|instance)\\b.*|"
            + ".*\\t/dataworld/.*\\t.*|"
            + ".*\\t/base/.*\\t.*"
            ;

    /**
     * Represents a name (i.e. the title) of a particular entity, it is used
     * for search
     */
    public static final String FREEBASE_NAME = "/type/object/name";
    /**
     * Represents an alias or alternative name for an entity
     */
    public static final String ALIAS = "/common/topic/alias";
    /**
     * Represents a domain (canonical) and skipts those created by the users
     */
    public static final String DOMAIN = "/freebase/domain_category/domains";
    /**
     * A topic domain is the domain associated to a topic.
     */
    public static final String TOPIC_DOMAIN = "/type/type/domain";
    /**
     * A notable type is the most probable type for an entity
     */
    public static final String NOTABLE_TYPE = "/common/topic/notable_types";
    /**
     * The description of a property and the associated type.
     */
    public static final String PROPERTY_SCHEMA_MATCHER = "/type/property/.*";
    /*
     * Link to the excpected type
     */
    public static final String PROPERTY_SCHEMA = "/type/property/expected_type";
    /**
     * Add reverse property is like Giugiaro designs Ferrari - Ferrari is designed_by Giugiaro
     */
    public static final String REVERSE_PROPERTY = "/type/property/reverse_property";
    /**
     * Identify the topic(s) of an entity (e.g. person, actor, ...)
     */
    public static final String ENTITY_TOPIC = "/type/object/type";
    /**
     * A document is usually a wikipedia link to an article. It can be useful
     * in case you want to expand the semantic of an entity.
     */
    public static final String DOCUMENT = "/common/document";
    /**
     * The uri of the document is probably the most important property.
     */
    public static final String DOCUMENT_URI = "/common/document/source_uri";
    /**
     * An external resource is a link to another representation of the same
     * entity
     */
    public static final String EXTERNAL_RESOURCES = "/type/object/key";
    /**
     * This pattern matches subj relation obj for non-property objects
     * and not previosly pruned by other relations.
     */
    public static final String RELATION_MATCHER = "/m/.*\\t.*\\t/m/.*";
    /**
     * This is the identifier for english language statements
     */
    public static final String ENGLISH_LANGUAGE = "/lang/en";
    /**
     * All the things that are non-id or values
     */
    public static final String NON_ID_OR_VALUES = "/.*/.*";
    /**
     * Matches all the possible machine ids
     */
    //public static final String MID_MATCHER = "/m/.*";
    public static final String MID_MATCHER = "[0-9]+";
    /**
     * Represents all the entities associated to a given type
     */
    public static final String TYPE_INSTANCE = "/type/type/instance";
    /**
     * Matches an article in wikipedia. Call convertToWikipediaLink to refer to the page
     */
    public static final String WIKIPEDIA_ARTICLE_MATCHER = "http://wp/en/[0-9]+";
    /**
     * The name of an isA relationship
     */
    public static final String ISA = "isA";
    /**
     * The name of a hasDomain relationship
     */
    public static final String HAS_DOMAIN = "hasDomain";
    /**
     * File separator of the datadump
     */
    public static final String SEPARATOR = "\t";
    /**
     * The mid of isA relationship
     */
    public static final long ISA_ID = 6848; //0;
    /**
     * The mid of hasDomain relationship
     */
    public static final long HAS_DOMAIN_ID = 495548; //1;



    
    /**
     * Convert a mid into a BigInteger since a mid is not more than "/m/"
     * followed by lower-case letters, digits and _, so it is a base-32 code
     * that can be easily converted to binary and then to bigint.
     *
     * ** NOTE ** Engineering version
     * @param mid The original freebase mid
     * @return the converted number
     * @throws NullPointerException
     * @throws IndexOutOfBoundsException
     */
    public static Long convertMidToLong(String mid) throws NullPointerException, IndexOutOfBoundsException {
        String id = mid.substring(mid.lastIndexOf('/') + 1).toUpperCase();
        long retval;
        String number = "";

        for (int i = 0; i < id.length(); i++) {
            number = (int)id.charAt(i) + number;
        }
        retval = Long.valueOf(number);
        return retval;
    }

    /**
     * Opposite of <code>convertMidToBigInt</code>
     * @param decimal
     * @return
     * @throws NullPointerException
     * @throws IndexOutOfBoundsException
     */
    public static String convertLongToMid(long decimal) throws NullPointerException, IndexOutOfBoundsException {
        String mid = "";
        String decimalString = decimal + "";

        for (int i = 0; i < decimalString.length(); i+= 2) {
            if(decimalString.length() < 5 ){
                mid = decimalString;
            } else {
                mid = (char)Integer.parseInt(decimalString.substring(i, i + 2)) + mid;
            }


        }

        return "/m/" + mid.toLowerCase();
    }

    /**
     * Convert a property mid into a long id used in the graph
     * @param mid The mid od the property
     * @return The long corresponding to this property    
     * @throws java.io.IOException    
     */
    public Long getPropertyId(String mid) throws IOException {        
        return propertiesToId.getOrDefault(mid, null);
    }

    /**
     * Convert a property long id into a mid used in the graph
     * @param id The id of the property
     * @return The string corresponding to the property label     
     * @throws java.io.IOException     
     */
    public String getPropertyMid(long id) throws IOException {
        return idToProperties.getOrDefault(id, convertLongToMid(id));
    }

    
    /**
     * 
     * @return
     * @throws IOException 
     */
    public Map<Long, String> getPropertiesToIdMap() throws IOException {
        return idToProperties;                
    }   
    
    
    private void loadProperties() throws IOException {
        propertiesToId = HashObjLongMaps.<String>newUpdatableMap();
        idToProperties = HashLongObjMaps.<String>newUpdatableMap();
        BufferedReader reader = null;
        try {
            String line;
            String[] splittedLine;

            InputStream propertyFileStream;
            if(propertiesFilePath == null){
                propertyFileStream = FreebaseNames.class.getResourceAsStream("/fb-properties.txt");
            }   else {
                propertyFileStream = new FileInputStream(propertiesFilePath);
            }         
            
            reader = new BufferedReader(new InputStreamReader(propertyFileStream));
            while ((line = reader.readLine()) != null) {
                if (line.length() > 1) {
                    splittedLine = StringUtils.fastSplit(line, ' ', 2);
                    propertiesToId.put(splittedLine[0], Long.parseLong(splittedLine[1]));
                    idToProperties.put(Long.parseLong(splittedLine[1]), splittedLine[0]);
                }
            }
        } catch (IOException ex) {
            throw ex;
        } finally {
            StringUtils.close(reader);
        }

    }
    

    
/**
     * Convert a wikipedia link from freebase format to proper wikipedia urls.
     * @param link The Freebase internal wikipedia format
     * @return The proper url of the wikipedia article
     */
    public static String convertFreebaseToWikipediaLink(String link) {
        String wikiLink = null;
        if (link != null && link.matches(WIKIPEDIA_ARTICLE_MATCHER)) {
            wikiLink = String.format("http://en.wikipedia.org/wiki?curid=%s",link.substring(link.lastIndexOf('/') + 1));
        }
        return wikiLink;
    }
    

}

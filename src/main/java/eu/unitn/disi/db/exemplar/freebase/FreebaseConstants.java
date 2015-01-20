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
package eu.unitn.disi.db.exemplar.freebase;

import eu.unitn.disi.db.grava.utils.Utilities;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Represents the different notations that are used in the Freebase data-dump files
 * They can be changed or embedded in a configuration file
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class FreebaseConstants {

    private FreebaseConstants() {
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

        private String freebaseId;

        private FreebaseBasicType(String freebaseId) {
            this.freebaseId = freebaseId;
        }

        public String freebaseId() {
            return freebaseId;
        }
    }

    private static HashMap<String,Long> propertiesToId = null;
    private static HashMap<Long,String> idToProperties = null;


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
     * Matches an article in wikipedi. Call convertToWikipediaLink to refer to the page
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
    public static long convertMidToLong(String mid) throws NullPointerException, IndexOutOfBoundsException {
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
            if(decimalString.length() < 3 ){
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
     * @throws IOException Since it loads the property from file return an exception if something wrong happens
     */
    public static long getPropertyId(String mid) throws IOException {
        if (propertiesToId == null) {
            loadProperties();
        }
        return propertiesToId.get(mid);
    }

    /**
     * Convert a property long id into a mid used in the graph
     * @param id The id of the property
     * @return The string corresponding to the property label
     * @throws IOException Since it loads the property from file return an exception if something wrong happens
     */
    public static String getPropertyMid(long id) throws IOException {
        if (idToProperties == null) {
            loadProperties();
        }
        return idToProperties.get(id);
    }

    private static void loadProperties() throws IOException {
        propertiesToId = new HashMap<String, Long>();
        idToProperties = new HashMap<Long, String>();
        BufferedReader reader = null;
        try {
            String line = null;
            String[] splittedLine;

            reader = new BufferedReader(new InputStreamReader(FreebaseConstants.class.getResourceAsStream("/fb-properties.txt")));
            while ((line = reader.readLine()) != null) {
                if (line.length() > 0) {
                    splittedLine = Utilities.fastSplit(line, ' ', 2);
                    propertiesToId.put(splittedLine[0], Long.parseLong(splittedLine[1]));
                    idToProperties.put(Long.parseLong(splittedLine[1]), splittedLine[0]);
                }
            }
        } catch (IOException ex) {
            throw ex;
        } finally {
            Utilities.close(reader);
        }
    }


//    public static void convertQueryGraph(Multigraph graph) throws NullPointerException, IndexOutOfBoundsException {
//        Collection<Long> vertices = graph.vertexSet();
//
//        for (Long vertex : vertices) {
//            vertex.setId(convertMidToBigInt(vertex.getId()).longValue() + "");
//        }
//    }




//    private static final String MID_MAPPING = "bcdfghjklmnpqrstvwxyz0123456789_";
//
//    private static final Map<Character, Integer> INVERSE_MAPPING = new HashMap<Character, Integer>();
//
//    static {
//        INVERSE_MAPPING.put('b', 0);
//        INVERSE_MAPPING.put('c', 1);
//        INVERSE_MAPPING.put('d', 2);
//        INVERSE_MAPPING.put('f', 3);
//        INVERSE_MAPPING.put('g', 4);
//        INVERSE_MAPPING.put('h', 5);
//        INVERSE_MAPPING.put('j', 6);
//        INVERSE_MAPPING.put('k', 7);
//        INVERSE_MAPPING.put('l', 8);
//        INVERSE_MAPPING.put('m', 9);
//        INVERSE_MAPPING.put('n', 10);
//        INVERSE_MAPPING.put('p', 11);
//        INVERSE_MAPPING.put('q', 12);
//        INVERSE_MAPPING.put('r', 13);
//        INVERSE_MAPPING.put('s', 14);
//        INVERSE_MAPPING.put('t', 15);
//        INVERSE_MAPPING.put('v', 16);
//        INVERSE_MAPPING.put('w', 17);
//        INVERSE_MAPPING.put('x', 18);
//        INVERSE_MAPPING.put('y', 19);
//        INVERSE_MAPPING.put('z', 20);
//        INVERSE_MAPPING.put('0', 21);
//        INVERSE_MAPPING.put('1', 22);
//        INVERSE_MAPPING.put('2', 23);
//        INVERSE_MAPPING.put('3', 24);
//        INVERSE_MAPPING.put('4', 25);
//        INVERSE_MAPPING.put('5', 26);
//        INVERSE_MAPPING.put('6', 27);
//        INVERSE_MAPPING.put('7', 28);
//        INVERSE_MAPPING.put('8', 29);
//        INVERSE_MAPPING.put('9', 30);
//        INVERSE_MAPPING.put('_', 31);
//    }

//    /**
//     * Convert a mid into a BigInteger since a mid is not more than "/m/"
//     * followed by lower-case letters, digits and _, so it is a base-32 code
//     * that can be easily converted to binary and then to bigint.
//     * @param mid The original freebase mid
//     * @return the converted number
//     * @throws NullPointerException
//     * @throws IndexOutOfBoundsException
//     */
//    public static BigInteger convertMidToBigInt(String mid) throws NullPointerException, IndexOutOfBoundsException {
//        String id = mid.substring(mid.lastIndexOf('/') + 1);
//        BigInteger retval = null;
//        String binaryString = "";
//        String binaryNumber;
//        int value;
//
//        for (int i = 0; i < id.length(); i++) {
//            value = INVERSE_MAPPING.get(id.charAt(i));
//            binaryNumber = Integer.toBinaryString(value);
//            binaryString = StringUtils.leftPad(binaryNumber, 5, '0') + binaryString;
//        }
//        retval = new BigInteger(binaryString, 2);
//        return retval;
//    }
//
//    /**
//     * Opposite of <code>convertMidToBigInt</code>
//     * @param decimal
//     * @return
//     * @throws NullPointerException
//     * @throws IndexOutOfBoundsException
//     */
//    public static String convertBigIntToMid(BigInteger decimal) throws NullPointerException, IndexOutOfBoundsException {
//        String mid = "";
//        //Base conversion algorithm.
//        BigInteger result = decimal, remainder, tmp;
//        final BigInteger THIRTY_TWO = BigInteger.valueOf(32);
//
//        while (!result.equals(BigInteger.ZERO)) {
//            tmp = result;
//            result = result.divide(THIRTY_TWO);
//            remainder = tmp.remainder(THIRTY_TWO);
//            mid += MID_MAPPING.charAt(remainder.intValue());
//        }
//
//        return "/m/" + mid;
//    }
}

/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.mvstore;

import java.util.HashMap;
import java.util.Map;

/**
 * Namespace for methods used to parse a Chunk encoded as a String.
 * A time efficient design intended to minimize the number of String objects
 * created during parsing and minimize the number of String comparisons.
 */
public class ChunkParser {
	public enum Field {
		// TODO Using Java 8 replace this field -> key mapping code with method references.

		// Serialized Chunk fields
		// Each enum value contains the serialized key name, the value type, and
		// a default value or a different field value to use as the default.
		// Any new values here need code added to the setField() method below.
		ID 				("chunk", Integer.class, 0), // ID requires a default value
		BLOCK 			("block", Long.class, 0),
		LEN 			("len", Integer.class, 0),
		PAGE_COUNT 		("pages", Integer.class, 0),
		PAGE_COUNT_LIVE ("livePages", Integer.class, PAGE_COUNT),
		MAP_ID 			("map", Integer.class, 0),
		MAX_LEN 		("max", Long.class, 0),
		MAX_LEN_LIVE	("liveMax", Long.class, MAX_LEN),
		META_ROOT_POS	("root", Long.class, 0),
		TIME			("time", Long.class, 0),
		UNUSED			("unused", Long.class, 0),
		VERSION			("version", Long.class, ID),
		NEXT			("next", Long.class, 0);

		private final String key;
    	private final Number defaultValue;
    	private final Field keyForDefaultValue;
    	private final Class<? extends Number> valueType;

		private Field(String key, Class<? extends Number> valueType, Number defaultValue) {
			this(key, valueType, defaultValue, null);
		}
		
		private Field(String key, Class<? extends Number> valueType, Field keyForDefaultValue) {
			this(key, valueType, null, keyForDefaultValue);
		}
		
		private Field(String key, Class<? extends Number> valueType, Number defaultValue, Field keyForDefaultValue) {
			this.key = key;
			this.valueType = valueType;
			this.defaultValue = defaultValue;
			this.keyForDefaultValue = keyForDefaultValue;
		}

		public Number getDefaultValue() {
			if (defaultValue == null) {
				return keyForDefaultValue.getDefaultValue();
			}
			
			return defaultValue;
		}
		
		/**
		 * Get a Field from a key name contained in a string.
		 * @param source The string containing the key name
		 * @param startPos Start position of the key name
		 * @param len Length of the key name
		 * @return The Field corresponding to the key name or null for no match.
		 */
		public static Field getField(String source, int startPos, int len) {
			// Other validation checks are done by regionMatches() below
			if (source == null || len < 1) {
				return null;
			}
			
			for (Field f: Field.values()) {
				if (f.key.length() == len && f.key.regionMatches(0, source, startPos, len)) {
					return f;
				}
			}
			
			return null;
		}
    }
	

	// Maximum length of a long represented as a hex string in a serialized Chunk
	private static final int STRING_LEN_MAX_VALUE = 2 * Long.SIZE / Byte.SIZE; // TODO Java 8 Use: Long.BYTES * 2;

	// Prevent instance creation
	private ChunkParser() {}
    
	/**
	 * Create a Chunk from its string representation.
	 * Set default values where needed.
	 * @param source A serialized Chunk
	 * @return The deserialized Chunk
	 */
    public static Chunk parse(String source) {
    	if (source == null || source.isEmpty()) {
    		assert (Field.ID.defaultValue != null) : "ID Field enum value has no default set";
    		return new Chunk(Field.ID.defaultValue.intValue());
    	}
    	
    	// Deserialize to a map
    	Map<Field, Number> mapChunkValues = parseMap(source);

        // Set Chunk field values
    	Number idVal = mapChunkValues.get(Field.ID);
    	if (idVal == null) {
    		idVal = getDefault(Field.ID, mapChunkValues);
    	}
        Chunk c = new Chunk(idVal.intValue());
        for (Field f: Field.values()) {
        	if (f != Field.ID) {
        		Number value = mapChunkValues.get(f);
    			setField(c, f, value != null ? value : getDefault(f, mapChunkValues));
        	}
        }
        
        return c;
    }
    
    // Deserialize a Chunk to a map
    private static Map<Field, Number> parseMap(String source) {
    	final Map<Field, Number> mapChunkValues = new HashMap<>();
    	final char[] value = new char[STRING_LEN_MAX_VALUE];

        for (int i = 0, size = source.length(); i < size;) {
            int startKey = i;
            i = source.indexOf(':', i);
            if (i < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT, "Not a map: {0}", source);
            }
            
            int valuePos = 0;
            Field field = Field.getField(source, startKey, i - startKey);
            i++;
            while (i < size) {
                char c = source.charAt(i++);
                if (c == ',') {
                    break;
                } else if (c == '\"') {
                    while (i < size) {
                        c = source.charAt(i++);
                        if (c == '\\') {
                            if (i == size) {
                                throw DataUtils.newIllegalStateException(
                                        DataUtils.ERROR_FILE_CORRUPT,
                                        "Not a map: {0}", source);
                            }
                            c = source.charAt(i++);
                        } else if (c == '\"') {
                            break;
                        }

                        // We're ignoring unrecognized keys
                        if (field != null) {
		                    value[valuePos++] = c;
		                    
		                    if (valuePos == STRING_LEN_MAX_VALUE) {
		                        throw DataUtils.newIllegalStateException(
		                                DataUtils.ERROR_FILE_CORRUPT,
		                                "Value for key {0} is too long in {1}", field.key, source);
		                    }
                        }
                    }
                } else if (field != null) { // We're ignoring unrecognized keys
                	value[valuePos++] = c;
                    
                    if (valuePos == STRING_LEN_MAX_VALUE) {
                        throw DataUtils.newIllegalStateException(
                                DataUtils.ERROR_FILE_CORRUPT,
                                "Value for key {0} is too long in {1}", field.key, source);
                    }
                }
            }

            if (field != null) {
	            // Parse the hex string into a Number
	            
	            // TODO With Java 8 use DataUtils.parse* method references
	            // instead of the 'valueType'.
	            if (Long.class.isAssignableFrom(field.valueType)) {
	            	mapChunkValues.put(field, new Long(DataUtils.parseHexLong(value, valuePos)));
	            } else if (Integer.class.isAssignableFrom(field.valueType)) {
	            	mapChunkValues.put(field, new Integer(DataUtils.parseHexInt(value, valuePos)));
	            } else {
	            	throw DataUtils.newIllegalStateException(
	                        DataUtils.ERROR_INTERNAL,
	                        "Unknown value type: {0}", field.valueType.getName());
	            }
            }
        }
        
        return mapChunkValues;
    }
	
    // Set the specified Chunk field
	private static Chunk setField(Chunk chunk, Field field, Number value) {
		switch (field) {
		case BLOCK:
			chunk.block = value.longValue();
			break;
		case LEN:
			chunk.len = value.intValue();
			break;
		case MAP_ID:
			chunk.mapId = value.intValue();
			break;
		case MAX_LEN:
			chunk.maxLen = value.longValue();
			break;
		case MAX_LEN_LIVE:
			chunk.maxLenLive = value.longValue();
			break;
		case META_ROOT_POS:
			chunk.metaRootPos = value.longValue();
			break;
		case NEXT:
			chunk.next = value.longValue();
			break;
		case PAGE_COUNT:
			chunk.pageCount = value.intValue();
			break;
		case PAGE_COUNT_LIVE:
			chunk.pageCountLive = value.intValue();
			break;
		case TIME:
			chunk.time = value.longValue();
			break;
		case UNUSED:
			chunk.unused = value.longValue();
			break;
		case VERSION:
			chunk.version = value.longValue();
			break;
		default:
			break;
		
		}
		
		return chunk;
	}
	
	// Get the default value for a Chunk field.
	// This should only be called after deserialization so that all serialized fields are set.
	private static Number getDefault(Field f, Map<Field, Number> mapChunkValues) {
		if (f.defaultValue != null) {
			return f.defaultValue;
		} else if (f.keyForDefaultValue != null) {
			Number n = mapChunkValues.get(f.keyForDefaultValue);
			if (n != null) {
				return n;
			} else {
				return getDefault(f.keyForDefaultValue, mapChunkValues);
			}
		} else {
        	throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "No value set or default value found for {0}", f.key);
    	}
	}

}

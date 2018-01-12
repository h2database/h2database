/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.mvstore.Chunk;
import org.h2.mvstore.ChunkParser;
import org.h2.mvstore.DataUtils;
import org.h2.test.TestBase;

/**
 * Tests the MVStore.
 */
public class TestChunkParser extends TestBase {
    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.config.big = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
    	testFieldExists();
    	testGetField();
    	testParse();
    	testParseDefaults();
    	testParseAllowedBadFormat();
    	testParseBadFormat();
    	testWrongCaseKeys();
    }

    private void testFieldExists() {
    	ChunkParser.Field f = ChunkParser.Field.valueOf("BLOCK");
    	assertTrue(f.equals(ChunkParser.Field.BLOCK));
    }
    
    private void testGetField() {
    	assertEquals(ChunkParser.Field.ID, ChunkParser.Field.getField("chunk", 0, 5));
    	assertEquals(ChunkParser.Field.PAGE_COUNT_LIVE, ChunkParser.Field.getField("livePages", 0, 9));
    	assertEquals(ChunkParser.Field.NEXT, ChunkParser.Field.getField("next", 0, 4));

    	assertNull(ChunkParser.Field.getField("NoSuchField", 0, 11));
    	assertNull(ChunkParser.Field.getField("map", 0, 100));
    	assertNull(ChunkParser.Field.getField("map", 1000, 3));
    	assertNull(ChunkParser.Field.getField("map", -1, 3));
    	assertNull(ChunkParser.Field.getField("map", 0, 0));
    	assertNull(ChunkParser.Field.getField("map", 0, -1));
    	assertNull(ChunkParser.Field.getField("maps", 0, 4));
    	assertNull(ChunkParser.Field.getField("xmap", 0, 4));
    	assertNull(ChunkParser.Field.getField("xmap", 1, 2));
    	
    	assertEquals(ChunkParser.Field.MAP_ID, ChunkParser.Field.getField("maps", 0, 3));
    	assertEquals(ChunkParser.Field.MAP_ID, ChunkParser.Field.getField("xmap", 1, 3));
    	assertEquals(ChunkParser.Field.MAP_ID, ChunkParser.Field.getField("xmaps", 1, 3));
    	assertEquals(ChunkParser.Field.MAP_ID, ChunkParser.Field.getField(
    			"A big long string with the word map in it at position 32", 32, 3));
    }
    
    private void testParseAllowedBadFormat() {
    	String source = "chunk:\"\\12";
    	checkChunk(ChunkParser.parse(source), 0x12);
    	
    	source = "chunk:12\"";
    	checkChunk(ChunkParser.parse(source), 0x12);
    	
    	source = "chunk:\"\"";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "chunk:\"\"12\"";
    	checkChunk(ChunkParser.parse(source), 0x12);
    	
    	source = "chunk:\"\",";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "chunk:";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "chunk:,";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "chunk:,nosuchkey:";
    	checkChunk(ChunkParser.parse(source));

    	source = "nosuchkey:,chunk:";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "nosuchkey:";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "nosuchkey:,";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "::";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "chunk:,chunk:ab,chunk:1234";
    	checkChunk(ChunkParser.parse(source), 0x1234);
    }

    private void testParse() {
    	String source = "chunk:\"12\\34\"";
    	checkChunk(ChunkParser.parse(source), 0x1234);
    	
    	source = "chunk:\"\"1234\"\",";
    	checkChunk(ChunkParser.parse(source), 0x1234);

    	source = "";
    	checkChunk(ChunkParser.parse(source));
    	
    	source = null;
    	checkChunk(ChunkParser.parse(source));
    	
    	source = "pages:" + Integer.toHexString(Integer.MAX_VALUE);
    	checkChunk(ChunkParser.parse(source),
    			null,
    			null,
    			null,
    			Integer.MAX_VALUE,
    			Integer.MAX_VALUE,
    			null,
    			null,
    			null,
    			null,
    			null,
    			null,
    			null,
    			null
    			);

    	source = "block:abcd," +
    			"nosuchkey:no such data for this key," +
    			"id:q," +
    			"block:1234," +
    			"badkey:," +
    			"chunk:f00d," +
    			"map:0A," +
    			"version:beef";
    	checkChunk(ChunkParser.parse(source),
    			0xf00d,
    			0x1234L,
    			null,
    			null,
    			null,
    			0xa,
    			null,
    			null,
    			null,
    			null,
    			null,
    			0xbeefL,
    			null
    			);
    	
    	source = "next:12," +
    			"version:23," +
    			"unused:34," +
    			"time:45," +
    			"root:56," +
    			"liveMax:67," +
    			"max:78," +
    			"map:89," +
    			"livePages:9A," +
    			"pages:ab," +
    			"len:Bc," +
    			"block:cD," +
    			"chunk:DE";
    	checkChunk(ChunkParser.parse(source),
    			0xde, 0xcdL, 0xbc, 0xab, 0x9a, 0x89, 0x78L, 0x67L, 0x56L, 0x45L, 0x34L, 0x23L, 0x12L);
    	
    	source = "nosuchkey: blah blah blah ," + source + ",another no such key: blah";
    	checkChunk(ChunkParser.parse(source),
    			0xde, 0xcdL, 0xbc, 0xab, 0x9a, 0x89, 0x78L, 0x67L, 0x56L, 0x45L, 0x34L, 0x23L, 0x12L);

    	source = "next:1,next:2,next:3,map:4,next:5";
    	checkChunk(ChunkParser.parse(source),
    			null,
    			null,
    			null,
    			null,
    			null,
    			0x4,
    			null,
    			null,
    			null,
    			null,
    			null,
    			null,
    			0x5L
    			);
    }

    private void testParseDefaults() {
    	String source = "chunk:1234,pages:abcd,max:8888";
    	checkChunk(ChunkParser.parse(source),
    			0x1234,
    			null,
    			null,
    			0xabcd,
    			0xabcd,
    			null,
    			0x8888L,
    			0x8888L,
    			null,
    			null,
    			null,
    			0x1234L,
    			null
    			);

    	source = "version:1234,livePages:abcd,liveMax:8888";
    	checkChunk(ChunkParser.parse(source),
    			null,
    			null,
    			null,
    			null,
    			0xabcd,
    			null,
    			null,
    			0x8888L,
    			null,
    			null,
    			null,
    			0x1234L,
    			null
    			);
    }
    
    private void testParseBadFormat() {
    	String[] source = {
    			"chunk:\\",
    			"chunk:12\\",
    			"chunk:\\12",
    			"chunk:\"\"12\\34\"\",",
    			"chunk",
    			"chunk:\"\\,",
    			"chunk:\"12\\,",
    			"chunk:\",",
    			"chunk:xyz",
    			"chunk: 123",
    			"chunk:123456789",
    			"block:FFFFFFFFFFFFFFFF",
    			"block:12345678901234567",
    	};

    	for (String s: source) {
	        try {
	        	ChunkParser.parse(s);
	            fail();
	        } catch (IllegalStateException e) {
	            assertEquals(DataUtils.ERROR_FILE_CORRUPT,
	                    DataUtils.getErrorCode(e.getMessage()));
	        }
    	}
    }
    
    private void testWrongCaseKeys() {
    	String source = "Block:12,leN:12,livepages:12,ROOT:12";
    	checkChunk(ChunkParser.parse(source));
    }
    
    private void checkChunk(Chunk c) {
    	checkChunk(c, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }
    
    private void checkChunk(Chunk c, int id) {
    	checkChunk(c, id, null, null, null, null, null, null, null, null, null, null, new Long(id), null);
    }

    // Pass null to check for default value
    private void checkChunk(Chunk c,
    		Integer id,
    		Long block,
    		Integer len,
    		Integer pageCount,
    		Integer pageCountLive,
    		Integer mapId,
    		Long maxLen,
    		Long maxLenLive,
    		Long metaRootPos,
    		Long time,
    		Long unused,
    		Long version,
    		Long next) {
    	assertEquals(ChunkParser.Field.ID, id, c.id);
    	assertEquals(ChunkParser.Field.BLOCK, block, c.block);
    	assertEquals(ChunkParser.Field.LEN, len, c.len);
    	assertEquals(ChunkParser.Field.PAGE_COUNT, pageCount, c.pageCount);
    	assertEquals(ChunkParser.Field.PAGE_COUNT_LIVE, pageCountLive, c.pageCountLive);
    	assertEquals(ChunkParser.Field.MAP_ID, mapId, c.mapId);
    	assertEquals(ChunkParser.Field.MAX_LEN, maxLen, c.maxLen);
    	assertEquals(ChunkParser.Field.MAX_LEN_LIVE, maxLenLive, c.maxLenLive);
    	assertEquals(ChunkParser.Field.META_ROOT_POS, metaRootPos, c.metaRootPos);
    	assertEquals(ChunkParser.Field.TIME, time, c.time);
    	assertEquals(ChunkParser.Field.UNUSED, unused, c.unused);
    	assertEquals(ChunkParser.Field.VERSION, version, c.version);
    	assertEquals(ChunkParser.Field.NEXT, next, c.next);
    }
    
    private void assertEquals(ChunkParser.Field f, Number compareTo, Number test) {
    	if (compareTo == null) {
    		compareTo = f.getDefaultValue();
    	}
    	
    	assertEquals(compareTo.longValue(), test.longValue());
    }
}

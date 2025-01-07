package org.h2.command.jsonpath;

import org.junit.jupiter.api.Test;

import org.h2.util.json.JSONStringSource;
import org.h2.util.json.JSONTarget;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;

class JsonPathTokenizerTest {

	@Test
	void testPathTokenizer() {
		JsonPathTokenizer p1 = new JsonPathTokenizer( "$.tjak[0].*" );
		JsonPathTokenizer p2 = new JsonPathTokenizer( "$.\"tjak\"[0].*" );
		JsonPathTokenizer p3 = new JsonPathTokenizer( "$.\"tjak tjok\"[0].*" );

//		final JSONStringSource stringSource = new JSONStringSource("");
		JSONValue input = JSONStringSource.parse( "{\"tjak\":[{\"t\":\"v\"}]}", new JSONValueTarget() );
		JSONValue input2 = JSONStringSource.parse( "{\"tjak tjok\":[{\"t\":\"v\"}]}", new JSONValueTarget() );

		JSONValue result = p1.find( input );
		JSONValue result2 = p3.find( input2 );
	}
}

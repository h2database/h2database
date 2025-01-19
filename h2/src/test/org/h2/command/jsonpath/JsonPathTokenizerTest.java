package org.h2.command.jsonpath;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import org.h2.util.json.JSONStringSource;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;
import org.h2.value.Value;

class JsonPathTokenizerTest {

	@Test
	void testPathTokenizer() {
		JsonPathTokenizer p1 = new JsonPathTokenizer( "$.tjak[0].*", null);
		JsonPathTokenizer p2 = new JsonPathTokenizer( "$.\"tjak\"[0].*", null);
		JsonPathTokenizer p3 = new JsonPathTokenizer( "$.\"tjak tjok\"[0].*", null);

//		final JSONStringSource stringSource = new JSONStringSource("");
		JSONValue input = JSONStringSource.parse( "{\"tjak\":[{\"t\":\"v\"}]}", new JSONValueTarget() );
		JSONValue input2 = JSONStringSource.parse( "{\"tjak tjok\":[{\"t\":\"v\"}]}", new JSONValueTarget() );

		Value result = p1.find( input, null);
		Value result2 = p3.find( input2 , null);
	}
}

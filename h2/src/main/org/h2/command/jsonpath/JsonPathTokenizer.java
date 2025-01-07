package org.h2.command.jsonpath;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;

public class JsonPathTokenizer {
	private int index;
	private final String path;
	private final AbsoluteJsonPathEvaluationNode node;

	public JsonPathTokenizer(final String path) {
		this.path = path;
		node = new AbsoluteJsonPathEvaluationNode();
		index = 0;
		skipWhitespace();
		readAbsolutePath();
	}

	private void readAbsolutePath() {
		if (currentIncrement() == '$') {
			while ( isPathElement() ) {
				readPathElement();
			}
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private void readRelativePath() {
		if (currentIncrement() == '@') {
			while ( isPathElement() ) {
				readPathElement();
			}
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private void readPath() {
		if (current() == '$') {
			readAbsolutePath();
		} else if (current() == '@') {
			readRelativePath();
		} else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private void next() {
		index++;
	}

	private void next(final int number) {
		index += number;
	}

	private char current() {
		return path.charAt(index);
	}

	private char currentIncrement() {
		final char res = current();
		next();
		return res;
	}

	private String currentSubstring() {
		return path.substring(index);
	}

	private boolean isPathElement() {
		if (eojp()) {
			return false;
		}
		switch ( current() ) {
			case '.':
			case '[':
			case '?':
				return true;
			default:
				return false;
		}
	}

	private boolean eojp() {
		return index >= path.length();
	}

	private void readPathElement() {
		switch ( currentIncrement() ) {
			case '.':
				if ( current() == '.' ) {
					next();
					readProperty( );
				} else {
					readProperty( );
				}
				break;
			case '[':
				readArray();
				break;
			case '?':
				readFilter();
				break;
			default:
				throw new UnsupportedOperationException();
		}
	}

	private void readFilter() {
		skipWhitespace();
		if ( currentIncrement() == '(') {
			readExpression( );
		} else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private JsonPathEvaluationNode readExpression() {
		skipWhitespace();
		final char c = path.charAt( index );

		if (Character.isDigit( c )) {
			return readNumberLiteral();
		} else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private void readArray() {
		skipWhitespace();
		switch ( current() ) {
			case '@':
			case '$':
				readPath();
				break;
			case '*':
				next();
				this.node.addAction((abs, input) -> {
					if (input instanceof JSONArray) {
						return input;
					} else {
						throw new UnsupportedOperationException( "" );
					}
				});
				break;
			default:
				final JsonPathEvaluationNode expr = readExpression();
				this.node.addAction( (abs, input) -> {
					final JSONValue eval = expr.eval( new JSONValue[] { input } );
					if (input instanceof JSONArray && eval instanceof JSONNumber) {
						final int i = ( (JSONNumber) eval ).getBigDecimal().intValueExact();
						return ((JSONArray) input).getElement(i);
					} else {
						throw new UnsupportedOperationException( "" );
					}
				} );
		}
	}

	private void readProperty() {
		final Pattern keyPattern = Pattern.compile( "^([a-zA-Z_]\\w*)\\s*(\\(?)" );

		switch ( current()){
			case '"':
				final LiteralJsonPathEvaluationNode literalJsonPathEvaluationNode = readStringLiteral();
				node.addAction((abs, input) -> {
					if (input instanceof JSONObject) {
						final JSONValue val = literalJsonPathEvaluationNode.eval(null);
						if (val instanceof JSONString) {
							return ((JSONObject) input).getFirst(((JSONString) val).getString());
						} else {
							throw new UnsupportedOperationException( "" );
						}
					} else {
						throw new UnsupportedOperationException( "" );
					}
				});
				break;
			case '*':
				next();
				node.addAction((abs, input) -> {
					if (input instanceof JSONObject) {
						final JSONValueTarget target = new JSONValueTarget();
						target.startArray();
						for (final Map.Entry<String, JSONValue> a : ((JSONObject) input).getMembers()) {
							a.getValue().addTo( target );
						}
						target.endArray();
						return target.getResult();
					} else {
						throw new UnsupportedOperationException("");
					}
				} );
				break;
			default:
				final Matcher keyMatcher = keyPattern.matcher(currentSubstring());
				if ( keyMatcher.find() ) {
					next(keyMatcher.end());
					if (keyMatcher.group(2).isBlank()) {
						node.addAction(( (abs, input) -> {
							if (input instanceof JSONObject) {
								return ((JSONObject) input).getFirst(keyMatcher.group(1));
							} else {
								throw new UnsupportedOperationException("");
							}
						} ));
					} else {
						readMethodArguments();
					}
				}
				else {
					throw new UnsupportedOperationException( "" );
				}
		}
	}

	private void readMethodArguments() {
		skipWhitespace();
		while (path.charAt(index) != ')') {
			readExpression();
			if (path.charAt(index + 1 ) == ',') {
			}
		}
	}

	private LiteralJsonPathEvaluationNode readNumberLiteral() {
		final Pattern nmbLiteralPattern = Pattern.compile( "^(\\d+)\\D");
		final Matcher matcher = nmbLiteralPattern.matcher( path.substring( index ) );
		if (matcher.find()) {
			index += matcher.end();
			final JSONValueTarget target = new JSONValueTarget();
			target.valueNumber(new BigDecimal(matcher.group(1)));
			return new LiteralJsonPathEvaluationNode(target.getResult());
		} else {
			throw new UnsupportedOperationException( String.format( "Cannot parse number from %s", path.substring( index )));
		}
	}

	private LiteralJsonPathEvaluationNode readStringLiteral() {
		final Pattern strliteralPattern = Pattern.compile(
				"^\"(([^\\\\\"]|\\\\\"|\\\\/|\\\\b|\\\\f|\\\\n|\\\\r|\\\\t|\\\\u[a-fA-F0-9]{4})+)\"" );
		final Matcher matcher = strliteralPattern.matcher( path.substring( index ) );
		if ( matcher.find() ) {
			final JSONValueTarget target = new JSONValueTarget();
			target.valueString(matcher.group(1));
			index += matcher.end();
			return new LiteralJsonPathEvaluationNode(target.getResult());
		} else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private void skipWhitespace() {
		final Matcher matcher = Pattern.compile( "^(\\s*)" ).matcher( path.substring( index ) );
		if ( matcher.find() ) {
			index += matcher.end();
		}
	}

	public JSONValue find(final JSONValue input) {
		return node.eval( new JSONValue[] {input} );
	}

	public interface JsonPathAction {
		JSONValue flatmap(final JSONValue absolute, final JSONValue relative);
	}

	public interface JsonPathEvaluationNode {
		JSONValue eval(final JSONValue[] inputs);
	}

	public static class AbsoluteJsonPathEvaluationNode implements JsonPathEvaluationNode {
		private final List<JsonPathAction> actions = new ArrayList<>();

		public void addAction(final JsonPathAction action) {
			actions.add( action );
		}

		@Override
		public JSONValue eval(final JSONValue[] inputs) {
			JSONValue res = inputs[0];
			for (final JsonPathAction action : actions) {
				res = action.flatmap(inputs[0], res);
			}
			return res;
		}
	}

	public static class LiteralJsonPathEvaluationNode implements JsonPathEvaluationNode {
		private final JSONValue value;

		public LiteralJsonPathEvaluationNode(final JSONValue value) {
			this.value = value;
		}

		@Override
		public JSONValue eval(final JSONValue[] inputs) {
			return value;
		}
	}
}

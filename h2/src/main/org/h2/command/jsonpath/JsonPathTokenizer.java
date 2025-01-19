package org.h2.command.jsonpath;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.h2.engine.Session;
import org.h2.expression.function.JsonQueryFunction;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueVarchar;

public class JsonPathTokenizer {
	private int index;
	private final String path;
	private final AbsoluteJsonPathExpression node;

	public JsonPathTokenizer(final String path, final JsonPathCollector collector) {
		this.path = path;
		index = 0;
		skipWhitespace();
		node = readAbsolutePath( collector );
	}

	private AbsoluteJsonPathExpression readAbsolutePath(final JsonPathCollector collector) {
		final List<JsonPathAction> actions = new ArrayList<>();
		JsonPathAction lastAction = null;
		if ( currentIncrement() == '$' ) {
			while ( isPathElement() ) {
				lastAction = readPathElement();
				actions.add( lastAction );
			}
			if ( lastAction instanceof JsonPathMethod ) {
				return new AbsoluteJsonPathExpression(
						actions,
						(JsonPathCollector) lastAction
				);
			}
			else {
				return new AbsoluteJsonPathExpression(
						actions,
						collector
				);
			}
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private RelativeJsonPathExpression readRelativePath() {
		final List<JsonPathAction> actions = new ArrayList<>();
		if ( currentIncrement() == '@' ) {
			while ( isPathElement() ) {
				actions.add( readPathElement() );
			}
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
		return new RelativeJsonPathExpression( actions, new MultiValueJsonPathCollector( true, JsonQueryFunction.Wrapper.WITHOUT_WRAPPER, true) );
	}

	private void next() {
		index++;
	}

	private void next(final int number) {
		index += number;
	}

	private char current() {
		return path.charAt( index );
	}

	private char currentIncrement() {
		final char res = current();
		next();
		return res;
	}

	private String currentSubstring() {
		return path.substring( index );
	}

	private boolean isPathElement() {
		if ( eojp() ) {
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

	private JsonPathAction readPathElement() {
		switch ( currentIncrement() ) {
			case '.':
				if ( current() == '.' ) {
					next();
					return readProperty();
				}
				else {
					return readProperty();
				}
			case '[':
				return readArray();
			case '?':
				return readFilter();
			default:
				throw new UnsupportedOperationException();
		}
	}

	private JsonPathAction readFilter() {
		skipWhitespace();
		if ( currentIncrement() == '(' ) {
			final JsonPathExpression expr = readExpression( 0 );
			if ( currentIncrement() != ')' ) {
				throw new UnsupportedOperationException( "" );
			}
			else {
				return (cur, con) -> {
					final Value eval = expr.eval( cur, con );
					if ( eval.getBoolean() ) {
						return Stream.of( cur );
					}
					else {
						return Stream.empty();
					}
				};
			}
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private JsonPathExpression readExpression(final int precedence) {
		JsonPathExpression left = null;
		while ( true ) {
			skipWhitespace();
			final char c = current();
			final char n = peek();
			if ( Character.isDigit( c ) || c == '.' || ( c == '-' && left == null ) ) {
				left = readNumberLiteral();
			}
			else if ( c == '"' ) {
				left = readStringLiteral();
			}
			else if ((c == 't' && n == 'r') || c == 'f' || c == 'n' ) {
				left = readBooleanLiteral();
			}
			else if ( c == '+' || c == '*' || c == '/' || c == '%' || c == '-' ) {
				if ( precedence > getPrecedence( c ) ) {
					return left;
				}
				else {
					next();
					final JsonPathExpression right = readExpression( getPrecedence( c ) );
					left = new OperatorJsonPathEvaulationNode( getArithmicOperator( c ), left, right );
				}
			}
			else if ( c == '&' || c == '|' || c == '!' || c == '=' || c == '<' || c == '>' ) {
				if ( precedence > getPrecedence( c, n ) ) {
					return left;
				}
				else {
					next( getOperatorLength( c, n ) );
					final JsonPathExpression right = readExpression( getPrecedence( c, n ) );
					left = new OperatorJsonPathEvaulationNode( getBooleanOperator( c, n ), left, right );
				}
			}
			else if ( c == '(' ) {
				next();
				left = readExpression( 0 );
				if ( currentIncrement() != ')' ) {
					throw new UnsupportedOperationException( "" );
				}
			}
			else if ( c == '$' ) {
				if ( isAbsolutePath() ) {
					left = readAbsolutePath( new MultiValueJsonPathCollector( true, JsonQueryFunction.Wrapper.WITHOUT_WRAPPER, true) );
				}
				else {
					next();
					final Pattern keyPattern = Pattern.compile( "^([a-zA-Z_]\\w*)" );
					final Matcher matcher = keyPattern.matcher( currentSubstring() );
					if ( matcher.find() ) {
						next( matcher.end( 1 ) );
						left = new VariableJsonPathExpression( matcher.group( 1 ) );
					}
					else {
						throw new UnsupportedOperationException( "" );
					}
				}
			}
			else if ( c == '@' ) {
				left = readRelativePath();
			}
			else if ( c == 'l' ) {
				if(!currentSubstring().startsWith("last")) {
					throw new UnsupportedOperationException( "" );
				} else {
					next(4);
					return new LastIndexJsonPathEvaluationNode();
				}
			}
			else if ( c == 's' ) {
				// starts with
			}
			else if ( c == 'e' ) {
				// exists
			}
			else {
				break;
			}
		}
		return left;
	}

	private int getPrecedence(char c, char n) {
		if ( c == '&' && n == '&' ) {
			return 2;
		}
		else if ( c == '|' && n == '|' ) {
			return 2;
		}
		else if ( c == '=' && n == '=' ) {
			return 2;
		}
		else if ( ( c == '!' && n == '=' ) || ( c == '<' && n == '>' ) ) {
			return 2;
		}
		else if ( c == '<' && n == '=' ) {
			return 2;
		}
		else if ( c == '>' && n == '=' ) {
			return 2;
		}
		else if ( c == '<' ) {
			return 1;
		}
		else if ( c == '>' ) {
			return 1;
		}
		else if ( c == '!' ) {
			return 10;
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private int getOperatorLength(char c, char n) {
		if ( c == '&' && n == '&' ) {
			return 2;
		}
		else if ( c == '|' && n == '|' ) {
			return 2;
		}
		else if ( c == '=' && n == '=' ) {
			return 2;
		}
		else if ( ( c == '!' && n == '=' ) || ( c == '<' && n == '>' ) ) {
			return 2;
		}
		else if ( c == '<' && n == '=' ) {
			return 2;
		}
		else if ( c == '>' && n == '=' ) {
			return 2;
		}
		else if ( c == '<' ) {
			return 1;
		}
		else if ( c == '>' ) {
			return 1;
		}
		else if ( c == '!' ) {
			return 1;
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private JsonPathOperator getBooleanOperator(char c, char n) {
		if ( c == '&' && n == '&' ) {
			return (a, b, context) -> ValueBoolean.get( a.getBoolean() && b.getBoolean() );
		}
		else if ( c == '|' && n == '|' ) {
			return (a, b, context) -> ValueBoolean.get( a.getBoolean() || b.getBoolean() );
		}
		else if ( c == '!' && n != '=' ) {
			return (a, b, context) -> ValueBoolean.get( !b.getBoolean() );
		}
		else {
			return (a, b, context) -> {
				final int val = a.compareTo(
						b,
						context.getSession(),
						context.getSession().getDataHandler().getCompareMode()
				);
				final boolean comparison;
				if ( c == '=' && n == '=' ) {
					comparison = val == 0;
				}
				else if ( c == '!' || c == '<' && n == '>' ) {
					comparison = val != 0;
				}
				else if ( c == '<' && n == '=' ) {
					comparison = val <= 0;
				}
				else if ( c == '>' && n == '=' ) {
					comparison = val >= 0;
				}
				else if ( c == '<' ) {
					comparison = val < 0;
				}
				else if ( c == '>' ) {
					comparison = val > 0;
				}
				else {
					throw new UnsupportedOperationException( "" );
				}
				return ValueBoolean.get( comparison );
			};
		}
	}

	private JsonPathExpression readBooleanLiteral() {
		final Pattern nmbLiteralPattern = Pattern.compile( "^(true|false|null)" );
		final Matcher matcher = nmbLiteralPattern.matcher( path.substring( index ) );
		if ( matcher.find() ) {
			index += matcher.end();
			final Value target;
			if ( "null".equals( matcher.group() ) ) {
				target = ValueJson.NULL;
			}
			else if ( "true".equals( matcher.group() ) ) {
				target = ValueBoolean.TRUE;
			}
			else {
				target = ValueBoolean.FALSE;
			}
			return new LiteralJsonPathExpression( target );
		}
		else {
			throw new UnsupportedOperationException( String.format(
					"Cannot parse number from %s",
					path.substring( index )
			) );
		}
	}

	private int getPrecedence(char c) {
		switch ( c ) {
			case '+':
			case '-':
				return 1;
			case '*':
			case '/':
			case '%':
				return 2;
			default:
				throw new UnsupportedOperationException( "" );
		}
	}

	private JsonPathOperator getArithmicOperator(char c) {
		switch ( c ) {
			case '+':
				return (l, r, context) -> l.add( r );
			case '-':
				return (l, r, context) -> l.subtract( r );
			case '*':
				return (l, r, context) -> l.multiply( r );
			case '/':
				return (l, r, context) -> l.divide( r, TypeInfo.TYPE_NUMERIC_FLOATING_POINT );
			case '%':
				return (l, r, context) -> l.modulus( r );
			default:
				throw new UnsupportedOperationException( "" );
		}
	}

	private boolean isAbsolutePath() {
		if ( current() == '$' ) {
			final char c = peek();
			return c == '.' || c == '?' || c == '[';
		}
		else {
			return false;
		}
	}

	private char peek() {
		if ( index + 1 >= path.length() ) {
			return '\0';
		}
		else {
			return path.charAt( index + 1 );
		}
	}

	private JsonPathAction readArray() {
		final List<JsonPathAction> subscripts = new ArrayList<>();
		skipWhitespace();
		if (current() == '*') {
			next();
			skipWhitespace();
			if ( currentIncrement() != ']' ) {
				throw new UnsupportedOperationException( "" );
			}
			return (current, context) -> {
				if ( current instanceof JSONArray ) {
					return Arrays.stream( ( (JSONArray) current ).getArray() );
				}
				else {
					throw new UnsupportedOperationException( "" );
				}
			};
		} else {
			do {
				final JsonPathExpression left = readExpression( 0 );
				skipWhitespace();
				final char c = current();
				final char n = peek();
				if ( c == ',' || c == ']' ) {
					subscripts.add( (current, context) -> {
						final int element = left.eval( current, context ).getInt();
						if ( current instanceof JSONArray ) {
							return Stream.of( ( (JSONArray) current ).getElement( element ) );
						}
						else {
							throw new UnsupportedOperationException( "" );
						}
					} );
				}
				else if ( c == ':' || ( c == 't' && n == 'o' ) ) {
					next( c == ':' ? 1 : 2 );
					final JsonPathExpression toExpr = readExpression( 0 );
					skipWhitespace();
					subscripts.add( (current, context) -> {
						final int from = left.eval( current, context ).getInt();
						final int to = toExpr.eval( current, context ).getInt();
						if ( current instanceof JSONArray && from <= to) {
							return Stream.iterate( from, f -> f < to, f -> f + 1)
									.map(i -> ((JSONArray) current).getElement( i ));
						}
						else {
							throw new UnsupportedOperationException( "" );
						}
					} );
				}
				else {
					throw new UnsupportedOperationException( "" );
				}
				if (current() != ',' && current() != ']') {
					throw new UnsupportedOperationException("");
				}
			} while (currentIncrement() == ',');
			if (subscripts.size() == 1) {
				return subscripts.get( 0 );
			} else {
				return (current, context) -> subscripts.stream().flatMap(sub -> sub.flatmap(current, context));
			}
		}
	}

	private JsonPathAction readProperty() {
		final Pattern keyPattern = Pattern.compile( "^([a-zA-Z_]\\w*)(\\(?)" );

		switch ( current() ) {
			case '"':
				final LiteralJsonPathExpression literalJsonPathEvaluationNode = readStringLiteral();
				return ( (input, context) -> {
					if ( input instanceof JSONObject ) {
						final Value val = literalJsonPathEvaluationNode.eval( input, context );
						return Stream.of( ( (JSONObject) input ).getFirst( val.getString() ) );
					}
					else {
						throw new UnsupportedOperationException( "" );
					}
				} );
			case '*':
				next();
				return ( (input, context) -> {
					if ( input instanceof JSONObject ) {
						return Arrays.stream( ( (JSONObject) input ).getMembers() )
								.map( Map.Entry::getValue );
					}
					else {
						throw new UnsupportedOperationException( "" );
					}
				} );
			default:
				final Matcher keyMatcher = keyPattern.matcher( currentSubstring() );
				if ( keyMatcher.find() ) {
					next( keyMatcher.end() );
					if ( keyMatcher.group( 2 ).isBlank() ) {
						return ( ( (input, context) -> {
							if ( input instanceof JSONObject ) {
								return Stream.of( ( (JSONObject) input ).getFirst( keyMatcher.group( 1 ) ) );
							}
							else {
								throw new IllegalStateException( String.format(
										"Input is not a JSON object, but a %s",
										input.getClass().getSimpleName()
								) );
							}
						} ) );
					}
					else {
						final List<JsonPathExpression> jsonPathExpressions = readMethodArguments();
						return new JsonPathMethod( keyMatcher.group( 1 ), jsonPathExpressions );
					}
				}
				else {
					throw new UnsupportedOperationException( "" );
				}
		}
	}

	private List<JsonPathExpression> readMethodArguments() {
		final List<JsonPathExpression> arguments = new ArrayList<>();
		skipWhitespace();
		while (current() != ')') {
			arguments.add( readExpression( 0 ) );
			skipWhitespace();
			final char c = current();
			if ( c == ',' ) {
				next();
			} else if (c != ')') {
				throw new UnsupportedOperationException("");
			}
		}
		return arguments;
	}

	private LiteralJsonPathExpression readNumberLiteral() {
		final Pattern nmbLiteralPattern = Pattern.compile( "^(-?)(\\d+)" );
		final Matcher matcher = nmbLiteralPattern.matcher( path.substring( index ) );
		if ( matcher.find() ) {
			index += matcher.end();
			return new LiteralJsonPathExpression( ValueNumeric.getAnyScale( new BigDecimal( matcher.group() ) ) );
		}
		else {
			throw new UnsupportedOperationException( String.format(
					"Cannot parse number from %s",
					path.substring( index )
			) );
		}
	}

	private LiteralJsonPathExpression readStringLiteral() {
		final Pattern strliteralPattern = Pattern.compile(
				"^\"(([^\\\\\"]|\\\\\"|\\\\/|\\\\b|\\\\f|\\\\n|\\\\r|\\\\t|\\\\u[a-fA-F0-9]{4})+)\"" );
		final Matcher matcher = strliteralPattern.matcher( path.substring( index ) );
		if ( matcher.find() ) {
			index += matcher.end();
			return new LiteralJsonPathExpression( ValueVarchar.get( matcher.group( 1 ) ) );
		}
		else {
			throw new UnsupportedOperationException( "" );
		}
	}

	private void skipWhitespace() {
		final Matcher matcher = Pattern.compile( "^(\\s*)" ).matcher( path.substring( index ) );
		if ( matcher.find() ) {
			index += matcher.end();
		}
	}

	public Value find(final JSONValue input, final Session session) {
		final JsonPathEvaluationContext context = new JsonPathEvaluationContext( input, Map.of(), session);
		return node.eval(input, context);
	}

	public static class JsonPathEvaluationContext {
		private final JSONValue absolute;
		private final Map<String, Value> variables;
		private final Session session;

		public JsonPathEvaluationContext(
				final JSONValue absolute,
				final Map<String, Value> variables,
				final Session session) {
			this.absolute = absolute;
			this.variables = variables;
			this.session = session;
		}

		public JSONValue getAbsolute() {
			return absolute;
		}

		public Value getVariable(String variableName) {
			return variables.get( variableName );
		}

		public Session getSession() {
			return session;
		}
	}

	public interface JsonPathAction {
		Stream<JSONValue> flatmap(final JSONValue current, final JsonPathEvaluationContext context);
	}

	public interface JsonPathExpression {
		Value eval(final JSONValue currentNode, final JsonPathEvaluationContext context);
	}

	public interface JsonPathCollector {
		Value eval(final Stream<JSONValue> input, final JsonPathEvaluationContext context);
	}

	public static class MultiValueJsonPathCollector implements JsonPathCollector {
		private final boolean isValue;
		private final JsonQueryFunction.Wrapper wrapper;
		private final boolean omitQuotes;

		public MultiValueJsonPathCollector(final boolean isValue, final JsonQueryFunction.Wrapper wrapper, final boolean omitQuotes) {
			this.isValue = isValue;
			this.wrapper = wrapper;
			this.omitQuotes = omitQuotes;
		}

		@Override
		public Value eval(final Stream<JSONValue> input, final JsonPathEvaluationContext context) {
			final List<JSONValue> result = input.collect(Collectors.toList());
			if (result.isEmpty()) {
				return ValueNull.INSTANCE;
//				return onEmpty.handle( session );
			} else if (result.size() == 1) {
				final JSONValue singleValue = result.get(0);
				if (isValue) {
					if ( singleValue instanceof JSONNull ) {
						return ValueNull.INSTANCE;
					}
					else if ( singleValue instanceof JSONNumber ) {
						return ValueNumeric.getAnyScale( ( (JSONNumber) singleValue ).getBigDecimal() );
					}
					else if ( singleValue instanceof JSONString ) {
						return ValueVarchar.get( ( (JSONString) singleValue ).getString() );
					}
					else if (singleValue instanceof JSONBoolean ) {
						return ValueBoolean.get( ( (JSONBoolean) singleValue ).getBoolean() );
					}
					else {
						throw new UnsupportedOperationException( "" );
					}
				} else if ( JsonQueryFunction.Wrapper.WITHOUT_WRAPPER.equals( wrapper )) {
					if ( omitQuotes && singleValue instanceof JSONString ) {
						return ValueVarchar.get( ( ( (JSONString) singleValue ) ).getString() );
					}
					else {
						return ValueJson.fromJson( singleValue );
					}
				} else {
					if ( JsonQueryFunction.Wrapper.CONDITIONAL_WRAPPER.equals( wrapper ) && (singleValue instanceof JSONArray || singleValue instanceof JSONObject)) {
						return ValueJson.fromJson( singleValue );
					} else {
						// wrap
						final JSONValueTarget target = new JSONValueTarget();
						target.startArray();
						singleValue.addTo( target );
						target.endArray();
						return ValueJson.fromJson(target.getResult());
					}
				}
			} else {
				if (isValue || JsonQueryFunction.Wrapper.WITHOUT_WRAPPER.equals( wrapper )) {
					throw new UnsupportedOperationException( "" );
				} else {
					// wrap
					final JSONValueTarget target = new JSONValueTarget();
					target.startArray();
					result.forEach(res -> res.addTo(target));
					target.endArray();
					return ValueJson.fromJson(target.getResult());
				}
			}
		}
	}

	public static class LastIndexJsonPathEvaluationNode implements JsonPathExpression {
		@Override
		public Value eval(final JSONValue currentNode, final JsonPathEvaluationContext context) {
			if (currentNode instanceof JSONArray) {
				return ValueNumeric.getAnyScale(new BigDecimal (((JSONArray) currentNode ).length() - 1));
			} else {
				throw new UnsupportedOperationException("");
			}
		}
	}

	public static class OperatorJsonPathEvaulationNode implements JsonPathExpression {
		private final JsonPathOperator operator;
		private final JsonPathExpression left;
		private final JsonPathExpression right;

		public OperatorJsonPathEvaulationNode(
				final JsonPathOperator operator,
				final JsonPathExpression left,
				final JsonPathExpression right) {
			this.operator = operator;
			this.left = left;
			this.right = right;
		}

		@Override
		public Value eval(final JSONValue currentNode, final JsonPathEvaluationContext context) {
			final Value leftVal = left.eval( currentNode, context );
			final Value rightVal = right.eval( currentNode, context );
			return operator.eval( leftVal, rightVal, context );
		}
	}

	public static class AbsoluteJsonPathExpression implements JsonPathExpression {
		private final List<JsonPathAction> actions;
		private final JsonPathCollector finisher;

		public AbsoluteJsonPathExpression(final List<JsonPathAction> actions, final JsonPathCollector finisher) {
			this.actions = actions;
			this.finisher = finisher;
		}

		public Stream<JSONValue> flatmap(final JSONValue currentNode, final JsonPathEvaluationContext context) {
			Stream<JSONValue> strRes = Stream.of( context.getAbsolute() );
			for ( final JsonPathAction action : actions ) {
				strRes = strRes.flatMap( res -> action.flatmap( res, context ) );
			}
			return strRes;
		}

		@Override
		public Value eval(final JSONValue currentNode, final JsonPathEvaluationContext context) {
			return finisher.eval( flatmap( currentNode, context ), context );
		}
	}

	public static class LiteralJsonPathExpression implements JsonPathExpression {
		private final Value value;

		public LiteralJsonPathExpression(final Value value) {
			this.value = value;
		}

		@Override
		public Value eval(final JSONValue currentNode, final JsonPathEvaluationContext context) {
			return value;
		}
	}

	public interface JsonPathOperator {
		Value eval(final Value left, final Value right, final JsonPathEvaluationContext context);
	}

//	public enum JsonPathOperator {
//		MULTIPLY('*', '\0', 12, (BigDecimal a, BigDecimal b) -> a.multiply(b)),
//		DIVISION('/', '\0', 12, (BigDecimal a, BigDecimal b) -> a.divide( b, RoundingMode.HALF_UP)),
//		MODULUS('%', '\0', 12, (BigDecimal a, BigDecimal b) -> a.remainder( b)),
//
//		ADD('+', '\0', 11, (BigDecimal a, BigDecimal b) -> a.add(b)),
//		SUBTRACT('-', '\0', 11, (BigDecimal a, BigDecimal b) -> a.subtract(b)),
//
//		LESS('<', '\0', 9, (BigDecimal a, BigDecimal b) -> a.compareTo(b) < 0),
//		LESS_OR_EQUAL('<', '=', 9, (BigDecimal a, BigDecimal b) -> a.compareTo(b) <= 0),
//		GREATER('<', '\0', 9, (BigDecimal a, BigDecimal b) -> a.compareTo(b) > 0),
//		GREATER_OR_EQUAL('<', '=', 9, (BigDecimal a, BigDecimal b) -> a.compareTo(b) >= 0),
//
//		EQUALS( '=', '=', 8, Objects::equals),
//		NOT_EQUALS('!', '=', 8, (a, b) -> !Objects.equals( a, b )),
//		NOT_EQUALS_ALT('<', '>', 8, (a, b) -> !Objects.equals( a, b )),
//
//		AND('&', '&', 4, (Boolean a, Boolean b) -> a && b),
//		OR('|', '|', 3, (Boolean a, Boolean b) -> a || b);
//
//		private final char c1;
//		private final char c2;
//		private final int precedence;
//		private final BiFunction<?, ?, ?> operation;
//
//		JsonPathOperator(char c1, char c2, int precedence, final BiFunction<?, ?, ?> operation) {
//			this.c1 = c1;
//			this.c2 = c2;
//			this.precedence = precedence;
//			this.operation = operation;
//		}
//
//		public static JsonPathOperator get(final char c1, final char c2) {
//			for (final JsonPathOperator operator : JsonPathOperator.values()) {
//				if (operator.c1 == c1 && (operator.c2 == c2 || operator.c2 == '\0')) {
//					return operator;
//				}
//			}
//			throw new UnsupportedOperationException("");
//		}
//
//		public int getPrecedence() {
//			return precedence;
//		}
//
//		public BiFunction<?, ?, ?> getOperation() {
//			return operation;
//		}
//	}
}

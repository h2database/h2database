package org.h2.expression.function;

import org.h2.command.jsonpath.JsonPathTokenizer;
import org.h2.command.jsonpath.JsonPathTokenizer.MultiValueJsonPathCollector;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.OperationN;
import org.h2.util.json.JSONStringSource;
import org.h2.util.json.JSONValue;
import org.h2.util.json.JSONValueTarget;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueJson;

public class JsonQueryFunction extends OperationN implements NamedExpression {
	private final boolean isValue;
	private final Wrapper wrapper;
	private final boolean omitQuotes;
	private JsonPathTokenizer compiledPath;
	private JsonOnClause onEmpty;
	private JsonOnClause onError;

	public JsonQueryFunction(final Expression input, final Expression path, final boolean isValue, final Wrapper wrapper, final boolean omitQuotes) {
		super(new Expression[] { input, path});

		this.isValue = isValue;
		this.wrapper = wrapper;
		this.omitQuotes = omitQuotes;
	}

	@Override
	public String getName() {
		return "JSON_VALUE";
	}

	@Override
	public Expression optimize(final SessionLocal session) {
		if (args[1].isConstant()) {
			compiledPath = new JsonPathTokenizer(args[1].getValue( session ).getString(), new MultiValueJsonPathCollector( isValue, wrapper, omitQuotes ));
		}
		return this;
	}

	@Override
	public Value getValue(final SessionLocal session) {
		final JsonPathTokenizer path;
		if (compiledPath != null) {
			path = compiledPath;
		} else {
			path = new JsonPathTokenizer( args[1].getValue( session ).getString(), new MultiValueJsonPathCollector( isValue, wrapper, omitQuotes ) );
		}

		final Value inputAsValue = args[0].getValue( session );
		final JSONValue input;
		if (inputAsValue instanceof ValueJson) {
			input = ( (ValueJson) inputAsValue ).getDecomposition();
		} else {
			input = JSONStringSource.parse( inputAsValue.getString(), new JSONValueTarget() );
		}

		return path.find( input , session);
	}

	@Override
	public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
		builder.append(getName()).append('(');
		writeExpressions(builder, args, sqlFlags);
		return builder;
	}

	public void setExplicitReturnType(final TypeInfo returntype) {
		this.type = returntype;
	}

	public void setOnEmptyHandler(final JsonOnClause jsonOnClause) {
		this.onEmpty = jsonOnClause;
	}

	public void setOnErrorHandler(final JsonOnClause jsonOnClause) {
		this.onError = jsonOnClause;
	}

	public static abstract class JsonOnClause {
		public abstract Value handle(final SessionLocal session);
	}

	public static class JsonErrorOnClause extends JsonOnClause {
		@Override
		public Value handle(SessionLocal session) {
			throw new UnsupportedOperationException();
		}
	}

	public static class JsonExpressionOnClause extends JsonOnClause {
		private final Expression expression;

		public JsonExpressionOnClause(final Expression expression) {
			this.expression = expression;
		}

		@Override
		public Value handle(final SessionLocal session) {
			return expression.getValue( session );
		}
	}

	public enum Wrapper {
		WITHOUT_WRAPPER,
		CONDITIONAL_WRAPPER,
		UNCONDITIONAL_WRAPPER
	}
}

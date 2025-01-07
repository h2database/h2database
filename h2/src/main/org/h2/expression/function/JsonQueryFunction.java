package org.h2.expression.function;

import org.h2.command.jsonpath.JsonPathTokenizer;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.OperationN;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

public class JsonQueryFunction extends OperationN implements NamedExpression {
	private final boolean isValue;
	private JsonPathTokenizer compiledPath;
	private JsonOnClause onEmpty;
	private JsonOnClause onError;

	public JsonQueryFunction(final Expression input, final Expression path, final boolean isValue) {
		super(new Expression[] { input, path});

		this.isValue = isValue;
	}

	@Override
	public String getName() {
		return "JSON_VALUE";
	}

	@Override
	public Value getValue(final SessionLocal session) {
		final JsonPathTokenizer path;
		if (compiledPath != null) {
			path = compiledPath;
		} else {
			path = new JsonPathTokenizer( args[1].getValue( session ).getString());
		}

		final JSONValue input = args[0].getValue( session ).convertToAnyJson().getDecomposition();
		final JSONValue output = path.find( input );

		if (isValue) {
			if (output instanceof JSONNumber ) {
				return ValueDecfloat.get(((JSONNumber) output).getBigDecimal()).convertTo( type );
			} else if (output instanceof JSONString ) {
				return ValueVarchar.get(((JSONString) output).getString(), session).convertTo( type );
			} else if (output instanceof JSONBoolean ) {
				return ValueBoolean.get(((JSONBoolean) output).getBoolean()).convertTo( type );
			} else if (output instanceof JSONNull ) {
				return ValueNull.INSTANCE.convertTo( type );
			} else {
				throw new UnsupportedOperationException("");
			}
		} else {
			return ValueJson.fromJson( output );
		}
	}

	@Override
	public Expression optimize(final SessionLocal session) {
		if (args[1].isConstant()) {
			compiledPath = new JsonPathTokenizer(args[1].getValue( session ).getString());
		}
		return this;
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
}

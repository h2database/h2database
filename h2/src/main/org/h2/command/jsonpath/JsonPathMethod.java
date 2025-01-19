package org.h2.command.jsonpath;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Stream;

import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathAction;
import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathEvaluationContext;
import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathExpression;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.value.ExtTypeInfoNumeric;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueVarchar;

public class JsonPathMethod implements JsonPathAction, JsonPathExpression {
	private final String methodName;
	private final List<JsonPathExpression> arguments;

	public JsonPathMethod(final String methodName, final List<JsonPathExpression> arguments) {
		this.methodName = methodName;
		this.arguments = arguments;
	}

	@Override
	public Stream<JSONValue> flatmap(final JSONValue current, final JsonPathEvaluationContext context) {
		return Stream.of( current );
	}

	@Override
	public Value eval(final JSONValue singleValue, final JsonPathEvaluationContext context) {
		switch ( methodName ) {
			case "type":
				if (!arguments.isEmpty()) {
					throw new UnsupportedOperationException("");
				}
				return getType(singleValue);
			case "size":
				if (!arguments.isEmpty()) {
					throw new UnsupportedOperationException("");
				}
				return getSize(singleValue);
			case "ceiling":
				if (!arguments.isEmpty()) {
					throw new UnsupportedOperationException("");
				}
				return round( singleValue, RoundingMode.CEILING);
			case "floor":
				if (!arguments.isEmpty()) {
					throw new UnsupportedOperationException("");
				}
				return round( singleValue, RoundingMode.FLOOR);
			case "decimal":
				if (arguments.size() > 2) {
					throw new UnsupportedOperationException("");
				}
				final Value precision = arguments.get(0).eval( singleValue, context );
				final Value scale = arguments.size() == 2 ? arguments.get(1).eval( singleValue, context ) : null;
				return round( singleValue, precision, scale);
			case "abs":
				if (!arguments.isEmpty()) {
					throw new UnsupportedOperationException("");
				}
				return abs(singleValue);
			case "datetime":
			case "date":
			case "time":
			case "keyvalue":
			default:
				throw new UnsupportedOperationException("");
		}
	}

	private Value round(JSONValue singleValue, Value precision, Value scale) {
		if (singleValue instanceof JSONNumber) {
			final TypeInfo targetType = TypeInfo.getTypeInfo(
					Value.NUMERIC,
					precision.getLong(),
					scale == null ? -1 : scale.getInt(),
					ExtTypeInfoNumeric.DECIMAL
			);
			BigDecimal val = ((JSONNumber) singleValue).getBigDecimal();
			if (scale != null) {
				val = val.setScale(scale.getInt(), RoundingMode.HALF_UP);
			}
			return ValueNumeric.get(val).convertTo(targetType);
		} else {
			throw new UnsupportedOperationException("");
		}
	}

	private Value abs(final JSONValue singleValue) {
		if (singleValue instanceof JSONNumber) {
			return ValueNumeric.getAnyScale(((JSONNumber) singleValue).getBigDecimal().abs());
		} else {
			throw new UnsupportedOperationException("");
		}
	}

	private Value round(final JSONValue singleValue, final RoundingMode roundingMode) {
		if (singleValue instanceof JSONNumber) {
			return ValueNumeric.getAnyScale(((JSONNumber) singleValue ).getBigDecimal().setScale( 0, roundingMode));
		} else {
			throw new UnsupportedOperationException("");
		}
	}

	private Value getSize(final JSONValue singleValue) {
		if (singleValue instanceof JSONArray) {
			return ValueNumeric.getAnyScale( new BigDecimal(( (JSONArray) singleValue ).length()) );
		} else {
			throw new UnsupportedOperationException("");
		}
	}

	private Value getType(final JSONValue singleValue) {
		if (singleValue instanceof JSONArray ) {
			return ValueVarchar.get("array");
		} else if (singleValue instanceof JSONObject ) {
			return ValueVarchar.get("object");
		} else if (singleValue instanceof JSONNumber ) {
			return ValueVarchar.get("number");
		} else if (singleValue instanceof JSONString ) {
			return ValueVarchar.get("string");
		} else if (singleValue instanceof JSONBoolean ) {
			return ValueVarchar.get("boolean");
		} else if (singleValue instanceof JSONNull ) {
			return ValueVarchar.get("null");
		} else {
			return ValueNull.INSTANCE;
		}
	}
}

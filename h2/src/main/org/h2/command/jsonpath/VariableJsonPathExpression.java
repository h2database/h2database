package org.h2.command.jsonpath;

import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathEvaluationContext;
import org.h2.util.json.JSONValue;
import org.h2.value.Value;

public class VariableJsonPathExpression implements JsonPathTokenizer.JsonPathExpression {
	private final String variableName;

	public VariableJsonPathExpression(final String variableName) {
		this.variableName = variableName;
	}

	@Override
	public Value eval(final JSONValue current, final JsonPathEvaluationContext context) {
		return context.getVariable(variableName);
	}
}

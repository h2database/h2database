package org.h2.command.jsonpath;

import java.util.List;
import java.util.stream.Stream;

import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathAction;
import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathCollector;
import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathExpression;
import org.h2.command.jsonpath.JsonPathTokenizer.JsonPathEvaluationContext;
import org.h2.util.json.JSONValue;
import org.h2.value.Value;

public class RelativeJsonPathExpression implements JsonPathExpression {
	private final List<JsonPathAction> actions;
	private final JsonPathCollector finisher;

	public RelativeJsonPathExpression(final List<JsonPathAction> actions, final JsonPathCollector finisher) {
		this.actions = actions;
		this.finisher = finisher;
	}

	@Override
	public Value eval(final JSONValue currentNode, final JsonPathEvaluationContext context) {
		Stream<JSONValue> strRes = Stream.of(currentNode);
		for ( final JsonPathAction action : actions) {
			strRes = strRes.flatMap(res -> action.flatmap(res, context));
		}
		return finisher.eval(strRes, context);
	}
}

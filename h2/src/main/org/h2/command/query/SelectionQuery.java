package org.h2.command.query;

import java.util.ArrayList;

import org.h2.expression.Expression;

public interface SelectionQuery {
	void setDistinct(final Expression[] array);

	void setDistinct();

	void setExpressions(final ArrayList<Expression> expressions);

	void setFetch(final Expression fetch);

	void setFetchPercent(final boolean fetchPercent);

	void setWithTies(final boolean ties);
}

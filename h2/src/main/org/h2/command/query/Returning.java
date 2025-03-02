package org.h2.command.query;

import org.h2.api.ErrorCode;
import org.h2.command.dml.DataChangeStatement;
import org.h2.command.dml.DeltaChangeCollector;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.SessionLocal;
import org.h2.expression.*;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.h2.expression.Expression.WITHOUT_PARENTHESES;

public class Returning extends Query implements SelectionQuery {
	public final DataChangeStatement statement;

	public Returning(final SessionLocal session, final DataChangeStatement statement) {
		super( session );
		this.statement = statement;
	}

	@Override
	public boolean isUnion() {
		return false;
	}

	@Override
	protected ResultInterface queryWithoutCache(final long limit, final ResultTarget target) {
		statement.prepare();
		int columnCount = expressionArray.length;
		final LocalResult result = new LocalResult(session, expressionArray, columnCount, columnCount);
		result.setForDataChangeDeltaTable();
		statement.update(DeltaChangeCollector.returningDeltaChangeCollector(session, this, result));
		return result;
	}

	@Override
	public void init() {
		if (checkInit) {
			throw DbException.getInternalError();
		}
		expandColumnList();
		if ((visibleColumnCount = expressions.size()) > Constants.MAX_COLUMNS) {
			throw DbException.get( ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
		}
		resultColumnCount = expressions.size();

		if (withTies && !hasOrder()) {
			throw DbException.get(ErrorCode.WITH_TIES_WITHOUT_ORDER_BY);
		}

		statement.visit(new MapColumnVisitor());
		checkInit = true;
	}

	@Override
	public void prepareExpressions() {
		Mode.ExpressionNames expressionNames = session.getMode().expressionNames;
		if (expressionNames == Mode.ExpressionNames.ORIGINAL_SQL || expressionNames == Mode.ExpressionNames.POSTGRESQL_STYLE) {
			optimizeExpressionsAndPreserveAliases();
		} else {
            expressions.replaceAll(expression -> expression.optimize(session));
		}
		expressionArray = expressions.toArray(new Expression[0]);
	}

	@Override
	public void preparePlan() {
	}

	@Override
	public double getCost() {
		return 0;
	}

	@Override
	public HashSet<Table> getTables() {
		final HashSet<Table> tables = new HashSet<>();
		tables.add(statement.getTable());
		return tables;
	}

	@Override
	public void setForUpdate(ForUpdate forUpdate) {
		throw DbException.get(ErrorCode.RESULT_SET_READONLY);
	}

	@Override
	public void mapColumns(ColumnResolver resolver, int level, boolean outer) {
		for (final Expression e : expressions) {
			e.mapColumns(resolver, level, Expression.MAP_INITIAL);
		}
	}

	@Override
	public void setEvaluatable(TableFilter tableFilter, boolean b) {
	}

	@Override
	public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
	}

	@Override
	public boolean allowGlobalConditions() {
		return false;
	}

	@Override
	public boolean isEverything(final ExpressionVisitor visitor) {
		return false;
	}

	@Override
	public void updateAggregate(SessionLocal s, int stage) {
	}

	@Override
	public void fireBeforeSelectTriggers() {
	}

	@Override
	public void setDistinct(Expression[] array) {
	}

	@Override
	public void setDistinct() {
	}

	@Override
	public void setExpressions(ArrayList<Expression> expressions) {
		this.expressions = expressions;
	}

	@Override
	public StringBuilder getPlanSQL(StringBuilder builder, int sqlFlags) {
		writeWithList(builder, sqlFlags);
		// can not use the field sqlStatement because the parameter
		// indexes may be incorrect: ? may be in fact ?2 for a subquery
		// but indexes may be set manually as well
		Expression[] exprList = expressions.toArray(new Expression[0]);
		statement.getPlanSQL(builder, sqlFlags);
		builder.append("RETURNING");
		for (int i = 0; i < visibleColumnCount; i++) {
			if (i > 0) {
				builder.append(',');
			}
			builder.append('\n');
			StringUtils.indent( builder, exprList[i].getSQL( sqlFlags, WITHOUT_PARENTHESES), 4, false);
		}
		appendEndOfQueryToSQL(builder, sqlFlags, exprList);
		return builder;
	}

	private void optimizeExpressionsAndPreserveAliases() {
		for (int i = 0; i < expressions.size(); i++) {
			Expression original = expressions.get(i);
			/*
			 * TODO cannot evaluate optimized now, because some optimize()
			 * methods violate their contract and modify the original
			 * expression.
			 */
			Expression optimized;
			if (i < visibleColumnCount) {
				String alias = original.getAlias(session, i);
				optimized = original.optimize(session);
				if (!optimized.getAlias(session, i).equals(alias)) {
					optimized = new Alias( optimized, alias, true);
				}
			} else {
				optimized = original.optimize(session);
			}
			expressions.set(i, optimized);
		}
	}

	private void expandColumnList() {
		final ArrayList<Expression> exprList = new ArrayList<>();
		final List<ColumnResolver> columnResolvers = statement.visit(new CollectColumnResolversVisitor()).getResult();
		boolean hasExpanded = false;
		for (final Expression expr : expressions) {
			if (expr instanceof Wildcard) {
				exprList.addAll(((Wildcard) expr).expandExpressions(session, columnResolvers));
				hasExpanded = true;
			} else {
				exprList.add(expr);
			}
		}
		if (hasExpanded) {
			this.expressions = exprList;
		}
	}

	private static class CollectColumnResolversVisitor implements ColumnResolver.ColumnResolverVisitor {
		private final List<ColumnResolver> collectedResolvers = new ArrayList<>();

		@Override
		public void accept(final ColumnResolver columnResolver) {
			collectedResolvers.add(columnResolver);
		}

		public List<ColumnResolver> getResult() {
			return collectedResolvers;
		}
	}

	private class MapColumnVisitor implements ColumnResolver.ColumnResolverVisitor {
		@Override
		public void accept(final ColumnResolver f) {
			mapColumns(f, 0, false);
		}
	}
}

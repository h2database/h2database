package org.h2.command.query;

import org.h2.engine.SessionLocal;
import org.h2.table.TableFilter;

/** Homework 5
 * Determines the best join order by the following rules rather than considering every possible permutation.
 * The following are the two rules that need to be upheld:
 * 1) Never choose an order that would introduce a cartesian product join (joining two tables that do not
 *    have an explicit ON clause in the query)
 * 2) Choose the table with the lowest number of roles out of all of the potential next tables to add to our
 *    join order as permitted by rule 1
 */
public class RuleBasedJoinOrderPicker {
    final SessionLocal session;
    final TableFilter[] filters;

    public RuleBasedJoinOrderPicker(SessionLocal session, TableFilter[] filters) {
        this.session = session;
        this.filters = filters;
    }

    public TableFilter[] bestOrder() {
        // Implement rules here

        return filters;
    }
}

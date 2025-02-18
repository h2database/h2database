package org.h2.command.dml;

import org.h2.result.LocalResult;
import org.h2.result.ResultTarget;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.value.Value;

public interface DeltaChangeCollector {
	static DeltaChangeCollector defaultCollector(final ResultTarget result, final ResultOption statementResultOption) {
		return (action, resultOption, values) -> {
			if ( statementResultOption.equals( resultOption ) ) {
				result.addRow( values );
			}
		};
	}

	static DeltaChangeCollector generatedKeys(int[] indexes, LocalResult result) {
		return defaultCollector( new GeneratedKeysCollector( indexes, result ), ResultOption.FINAL );
	}

	static DeltaChangeCollector noop() {
		return (action, resultOption, values) -> {
		};
	}

	enum Action {
		DELETE,
		INSERT,
		UPDATE
	}

	void trigger(final Action action, final ResultOption resultOption, final Value[] values);

	/**
	 * Collector of generated keys.
	 */
	final class GeneratedKeysCollector implements ResultTarget {

		private final int[] indexes;
		private final LocalResult result;

		GeneratedKeysCollector(int[] indexes, LocalResult result) {
			this.indexes = indexes;
			this.result = result;
		}

		@Override
		public void limitsWereApplied() {
			// Nothing to do
		}

		@Override
		public long getRowCount() {
			// Not required
			return 0L;
		}

		@Override
		public void addRow(Value... values) {
			int length = indexes.length;
			Value[] row = new Value[length];
			for ( int i = 0; i < length; i++ ) {
				row[i] = values[indexes[i]];
			}
			result.addRow( row );
		}

	}
}

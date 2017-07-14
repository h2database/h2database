package org.h2.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.h2.command.Prepared;
import org.h2.constraint.Constraint;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObjectBase;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.value.CompareMode;
import org.h2.value.Value;

/**
 * Abstract base class for tables and table synonyms.
 */
public abstract class AbstractTable extends SchemaObjectBase {

    /**
     * Resolves the "real" table behind this abstract table. For table this is the table itself for
     * a synonym this is the backing table of the synonym. This method should be used in places, where synonym support
     * is desired.
     */
    public abstract Table resolve();

    /**
     * Returns the current table or fails with an unsupported database exception for synonyms.
     * This method should be used in places that do not support the usage of synonyms.
     */
    public abstract Table asTable();

    public abstract TableType getTableType();


}

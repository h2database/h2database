/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;

/**
 * A FILE_READ or FILE_WRITE function.
 */
public final class FileFunction extends Function1_2 {

    /**
     * FILE_READ() (non-standard).
     */
    public static final int FILE_READ = 0;

    /**
     * FILE_WRITE() (non-standard).
     */
    public static final int FILE_WRITE = FILE_READ + 1;

    private static final String[] NAMES = { //
            "FILE_READ", "FILE_WRITE" //
    };

    private final int function;

    public FileFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        session.getUser().checkAdmin();
        Value v1 = left.getValue(session);
        if (v1 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        switch (function) {
        case FILE_READ: {
            String fileName = v1.getString();
            Database database = session.getDatabase();
            try {
                long fileLength = FileUtils.size(fileName);
                ValueLob lob;
                try (InputStream in = FileUtils.newInputStream(fileName)) {
                    if (right == null) {
                        lob = database.getLobStorage().createBlob(in, fileLength);
                    } else {
                        Value v2 = right.getValue(session);
                        Reader reader = v2 == ValueNull.INSTANCE ? new InputStreamReader(in)
                                : new InputStreamReader(in, v2.getString());
                        lob = database.getLobStorage().createClob(reader, fileLength);
                    }
                }
                v1 = session.addTemporaryLob(lob);
            } catch (IOException e) {
                throw DbException.convertIOException(e, fileName);
            }
            break;
        }
        case FILE_WRITE: {
            Value v2 = right.getValue(session);
            if (v2 == ValueNull.INSTANCE) {
                v1 = ValueNull.INSTANCE;
            } else {
                String fileName = v2.getString();
                try (OutputStream fileOutputStream = Files.newOutputStream(Paths.get(fileName));
                        InputStream in = v1.getInputStream()) {
                    v1 = ValueBigint.get(IOUtils.copy(in, fileOutputStream));
                } catch (IOException e) {
                    throw DbException.convertIOException(e, fileName);
                }
            }
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        switch (function) {
        case FILE_READ:
            type = right == null ? TypeInfo.getTypeInfo(Value.BLOB, Integer.MAX_VALUE, 0, null)
                    : TypeInfo.getTypeInfo(Value.CLOB, Integer.MAX_VALUE, 0, null);
            break;
        case FILE_WRITE:
            type = TypeInfo.TYPE_BIGINT;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return false;
        case ExpressionVisitor.READONLY:
            if (function == FILE_WRITE) {
                return false;
            }
        }
        return super.isEverything(visitor);
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}

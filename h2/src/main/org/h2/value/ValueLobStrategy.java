package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.util.StringUtils;

/**
 * We need to be able to configure our ValueLob classes around two different
 * axes<br>
 * <ul>
 * <li>(a) a type axis like Json or Geometry
 * <li>(b) a fetch/storage strategy that indicates how to retrieve and store the
 * data. <br>
 * This class hierarchy represents (b), so that we can use subclassing for (a).
 */
public abstract class ValueLobStrategy {

    public abstract InputStream getInputStream(ValueLob lob);

    public abstract InputStream getInputStream(ValueLob lob, long oneBasedOffset, long length);

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param database the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    public ValueLob copy(ValueLob lob, DataHandler database, int tableId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision the new precision
     * @return the truncated or this value
     */
    ValueLob convertPrecision(ValueLob oldLob, long precision) {
        throw new UnsupportedOperationException();
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    public boolean isLinkedToTable() {
        return false;
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    public void remove(ValueLob lob) {}

    public int compareTypeSafe(ValueLob lob, Value v, CompareMode mode, CastDataProvider provider) {
        if (v == lob) {
            return 0;
        }
        ValueLob v2 = (ValueLob) v;
        return ValueLob.compare(lob, v2);
    }

    /**
     * Returns the data handler.
     *
     * @return the data handler, or {@code null}
     */
    public DataHandler getDataHandler() {
        return null;
    }

    /**
     * Create an independent copy of this value, that will be bound to a result.
     *
     * @return the value (this for small objects)
     */
    public ValueLob copyToResult(ValueLob lob) {
        return lob;
    }

    public String getString(ValueLob lob) {
        if (lob.valueType == ValueLob.CLOB) {
            if (lob.precision > Constants.MAX_STRING_LENGTH) {
                throw lob.getStringTooLong(lob.precision);
            }
            return lob.readString((int) lob.precision);
        }
        long p = lob.otherPrecision;
        if (p >= 0L) {
            if (p > Constants.MAX_STRING_LENGTH) {
                throw lob.getStringTooLong(p);
            }
            return lob.readString((int) p);
        }
        // 1 Java character may be encoded with up to 3 bytes
        if (lob.precision > Constants.MAX_STRING_LENGTH * 3) {
            throw lob.getStringTooLong(lob.charLength());
        }
        String s = lob.readString(Integer.MAX_VALUE);
        lob.otherPrecision = p = s.length();
        if (p > Constants.MAX_STRING_LENGTH) {
            throw lob.getStringTooLong(p);
        }
        return s;
    }

    public byte[] getBytes() {
        return null;
    }

    public byte[] getBytesNoCopy() {
        return null;
    }

    public StringBuilder getSQL(ValueLob lob, StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & ValueLob.REPLACE_LOBS_FOR_TRACE) != 0 && (!(this instanceof ValueLobStrategyInMemory)
                || lob.precision > SysProperties.MAX_TRACE_DATA_LENGTH)) {
            if (lob.valueType == Value.CLOB) {
                builder.append("SPACE(").append(lob.precision);
            } else {
                builder.append("CAST(REPEAT(CHAR(0), ").append(lob.precision).append(") AS BINARY VARYING");
            }
            ValueLobStrategyDatabase lobDb = (ValueLobStrategyDatabase) this;
            builder.append(" /* table: ").append(lobDb.getTableId()).append(" id: ").append(lobDb.getLobId())
                    .append(" */)");
        } else {
            if (lob.valueType == Value.CLOB) {
                if ((sqlFlags & (ValueLob.REPLACE_LOBS_FOR_TRACE | ValueLob.NO_CASTS)) == 0) {
                    StringUtils.quoteStringSQL(builder.append("CAST("), lob.getString())
                            .append(" AS CHARACTER LARGE OBJECT(").append(lob.precision).append("))");
                } else {
                    StringUtils.quoteStringSQL(builder, lob.getString());
                }
            } else {
                if ((sqlFlags & (ValueLob.REPLACE_LOBS_FOR_TRACE | ValueLob.NO_CASTS)) == 0) {
                    builder.append("CAST(X'");
                    StringUtils.convertBytesToHex(builder, lob.getBytesNoCopy()).append("' AS BINARY LARGE OBJECT(")
                            .append(lob.precision).append("))");
                } else {
                    builder.append("X'");
                    StringUtils.convertBytesToHex(builder, lob.getBytesNoCopy()).append('\'');
                }
            }
        }
        return builder;
    }

    public long charLength(ValueLob lob) {
        if (lob.valueType == ValueLob.CLOB) {
            return lob.precision;
        }
        long p = lob.otherPrecision;
        if (p < 0L) {
            try (Reader r = lob.getReader()) {
                p = 0L;
                for (;;) {
                    p += r.skip(Long.MAX_VALUE);
                    if (r.read() < 0) {
                        break;
                    }
                    p++;
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
            lob.otherPrecision = p;
        }
        return p;
    }

    public long octetLength(ValueLob lob) {
        if (lob.valueType == ValueLob.BLOB) {
            return lob.precision;
        }
        long p = lob.otherPrecision;
        if (p < 0L) {
            try (InputStream is = lob.getInputStream()) {
                p = 0L;
                for (;;) {
                    p += is.skip(Long.MAX_VALUE);
                    if (is.read() < 0) {
                        break;
                    }
                    p++;
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
            lob.otherPrecision = p;
        }
        return p;
    }

    public int getMemory() {
        return 140;
    }
}

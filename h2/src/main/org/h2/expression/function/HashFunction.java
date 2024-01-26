/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import static org.h2.util.Bits.INT_VH_BE;
import static org.h2.util.Bits.LONG_VH_BE;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.security.SHA3;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarbinary;

/**
 * A HASH or ORA_HASH function.
 */
public final class HashFunction extends FunctionN {

    /**
     * HASH() (non-standard).
     */
    public static final int HASH = 0;

    /**
     * ORA_HASH() (non-standard).
     */
    public static final int ORA_HASH = HASH + 1;

    private static final String[] NAMES = { //
            "HASH", "ORA_HASH" //
    };

    private final int function;

    public HashFunction(Expression arg, int function) {
        super(new Expression[] { arg });
        this.function = function;
    }

    public HashFunction(Expression arg1, Expression arg2, Expression arg3, int function) {
        super(arg3 == null ? new Expression[] { arg1, arg2 } : new Expression[] { arg1, arg2, arg3 });
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2, Value v3) {
        switch (function) {
        case HASH:
            v1 = getHash(v1.getString(), v2, v3 == null ? 1 : v3.getInt());
            break;
        case ORA_HASH:
            v1 = oraHash(v1, v2 == null ? 0xffff_ffffL : v2.getLong(), v3 == null ? 0L : v3.getLong());
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    private static Value getHash(String algorithm, Value value, int iterations) {
        if (iterations <= 0) {
            throw DbException.getInvalidValueException("iterations", iterations);
        }
        MessageDigest md;
        switch (StringUtils.toUpperEnglish(algorithm)) {
        case "MD5":
        case "SHA-1":
        case "SHA-224":
        case "SHA-256":
        case "SHA-384":
        case "SHA-512":
            md = hashImpl(value, algorithm);
            break;
        case "SHA256":
            md = hashImpl(value, "SHA-256");
            break;
        case "SHA3-224":
            md = hashImpl(value, SHA3.getSha3_224());
            break;
        case "SHA3-256":
            md = hashImpl(value, SHA3.getSha3_256());
            break;
        case "SHA3-384":
            md = hashImpl(value, SHA3.getSha3_384());
            break;
        case "SHA3-512":
            md = hashImpl(value, SHA3.getSha3_512());
            break;
        default:
            throw DbException.getInvalidValueException("algorithm", algorithm);
        }
        byte[] b = md.digest();
        for (int i = 1; i < iterations; i++) {
            b = md.digest(b);
        }
        return ValueVarbinary.getNoCopy(b);
    }

    private static Value oraHash(Value value, long bucket, long seed) {
        if ((bucket & 0xffff_ffff_0000_0000L) != 0L) {
            throw DbException.getInvalidValueException("bucket", bucket);
        }
        if ((seed & 0xffff_ffff_0000_0000L) != 0L) {
            throw DbException.getInvalidValueException("seed", seed);
        }
        MessageDigest md = hashImpl(value, "SHA-1");
        if (md == null) {
            return ValueNull.INSTANCE;
        }
        if (seed != 0L) {
            byte[] b = new byte[4];
            INT_VH_BE.set(b, 0, (int) seed);
            md.update(b);
        }
        long hc = (long) LONG_VH_BE.get(md.digest(), 0);
        // Strip sign and use modulo operation to get value from 0 to bucket
        // inclusive
        return ValueBigint.get((hc & Long.MAX_VALUE) % (bucket + 1));
    }

    private static MessageDigest hashImpl(Value value, String algorithm) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(algorithm);
        } catch (Exception ex) {
            throw DbException.convert(ex);
        }
        return hashImpl(value, md);
    }

    private static MessageDigest hashImpl(Value value, MessageDigest md) {
        try {
            switch (value.getValueType()) {
            case Value.VARCHAR:
            case Value.CHAR:
            case Value.VARCHAR_IGNORECASE:
                md.update(value.getString().getBytes(StandardCharsets.UTF_8));
                break;
            case Value.BLOB:
            case Value.CLOB: {
                byte[] buf = new byte[4096];
                try (InputStream is = value.getInputStream()) {
                    for (int r; (r = is.read(buf)) > 0;) {
                        md.update(buf, 0, r);
                    }
                }
                break;
            }
            default:
                md.update(value.getBytesNoCopy());
            }
            return md;
        } catch (Exception ex) {
            throw DbException.convert(ex);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        switch (function) {
        case HASH:
            type = TypeInfo.TYPE_VARBINARY;
            break;
        case ORA_HASH:
            type = TypeInfo.TYPE_BIGINT;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}

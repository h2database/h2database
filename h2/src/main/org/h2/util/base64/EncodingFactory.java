/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.base64;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * A factory to create new encoding schemes that represent binary data.
 */
public class EncodingFactory {

    private EncodingFactory() {
        // utility class
    }

    /**
     * Get realization of algorithm.
     *
     * @param algorithm Algorithm name
     * @return Realization of algorithm.
     */
    public static EncodingAlgorithm getEncodingAlgorithm(String algorithm) {
        if (algorithm==null || algorithm.isEmpty() || "BASE64".equalsIgnoreCase(algorithm)) {
            return new EncodingBase64();
        } else if ("URL".equalsIgnoreCase(algorithm)) {
            return new EncodingBase64Url();
        }
        throw DbException.get(ErrorCode.UNSUPPORTED_ENCODING_ALGORITHM, algorithm);
    }
}

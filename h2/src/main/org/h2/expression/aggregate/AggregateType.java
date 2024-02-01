/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

/**
 * The type of an aggregate function.
 */
public enum AggregateType {

    /**
     * The aggregate type for COUNT(*).
     */
    COUNT_ALL,

    /**
     * The aggregate type for COUNT(expression).
     */
    COUNT,

    /**
     * The aggregate type for SUM(expression).
     */
    SUM,

    /**
     * The aggregate type for MIN(expression).
     */
    MIN,

    /**
     * The aggregate type for MAX(expression).
     */
    MAX,

    /**
     * The aggregate type for AVG(expression).
     */
    AVG,

    /**
     * The aggregate type for STDDEV_POP(expression).
     */
    STDDEV_POP,

    /**
     * The aggregate type for STDDEV_SAMP(expression).
     */
    STDDEV_SAMP,

    /**
     * The aggregate type for VAR_POP(expression).
     */
    VAR_POP,

    /**
     * The aggregate type for VAR_SAMP(expression).
     */
    VAR_SAMP,

    /**
     * The aggregate type for ANY_VALUE(expression).
     */
    ANY_VALUE,

    /**
     * The aggregate type for ANY(expression).
     */
    ANY,

    /**
     * The aggregate type for EVERY(expression).
     */
    EVERY,

    /**
     * The aggregate type for BIT_AND_AGG(expression).
     */
    BIT_AND_AGG,

    /**
     * The aggregate type for BIT_OR_AGG(expression).
     */
    BIT_OR_AGG,

    /**
     * The aggregate type for BIT_XOR_AGG(expression).
     */
    BIT_XOR_AGG,

    /**
     * The aggregate type for BIT_NAND_AGG(expression).
     */
    BIT_NAND_AGG,

    /**
     * The aggregate type for BIT_NOR_AGG(expression).
     */
    BIT_NOR_AGG,

    /**
     * The aggregate type for BIT_XNOR_AGG(expression).
     */
    BIT_XNOR_AGG,

    /**
     * The aggregate type for HISTOGRAM(expression).
     */
    HISTOGRAM,

    /**
     * The aggregate type for COVAR_POP binary set function.
     */
    COVAR_POP,

    /**
     * The aggregate type for COVAR_SAMP binary set function.
     */
    COVAR_SAMP,

    /**
     * The aggregate type for CORR binary set function.
     */
    CORR,

    /**
     * The aggregate type for REGR_SLOPE binary set function.
     */
    REGR_SLOPE,

    /**
     * The aggregate type for REGR_INTERCEPT binary set function.
     */
    REGR_INTERCEPT,

    /**
     * The aggregate type for REGR_COUNT binary set function.
     */
    REGR_COUNT,

    /**
     * The aggregate type for REGR_R2 binary set function.
     */
    REGR_R2,

    /**
     * The aggregate type for REGR_AVGX binary set function.
     */
    REGR_AVGX,

    /**
     * The aggregate type for REGR_AVGY binary set function.
     */
    REGR_AVGY,

    /**
     * The aggregate type for REGR_SXX binary set function.
     */
    REGR_SXX,

    /**
     * The aggregate type for REGR_SYY binary set function.
     */
    REGR_SYY,

    /**
     * The aggregate type for REGR_SXY binary set function.
     */
    REGR_SXY,

    /**
     * The type for RANK() hypothetical set function.
     */
    RANK,

    /**
     * The type for DENSE_RANK() hypothetical set function.
     */
    DENSE_RANK,

    /**
     * The type for PERCENT_RANK() hypothetical set function.
     */
    PERCENT_RANK,

    /**
     * The type for CUME_DIST() hypothetical set function.
     */
    CUME_DIST,

    /**
     * The aggregate type for PERCENTILE_CONT(expression).
     */
    PERCENTILE_CONT,

    /**
     * The aggregate type for PERCENTILE_DISC(expression).
     */
    PERCENTILE_DISC,

    /**
     * The aggregate type for MEDIAN(expression).
     */
    MEDIAN,

    /**
     * The aggregate type for LISTAGG(...).
     */
    LISTAGG,

    /**
     * The aggregate type for ARRAY_AGG(expression).
     */
    ARRAY_AGG,

    /**
     * The aggregate type for MODE(expression).
     */
    MODE,

    /**
     * The aggregate type for ENVELOPE(expression).
     */
    ENVELOPE,

    /**
     * The aggregate type for JSON_OBJECTAGG(expression: expression).
     */
    JSON_OBJECTAGG,

    /**
     * The aggregate type for JSON_ARRAYAGG(expression).
     */
    JSON_ARRAYAGG,

}

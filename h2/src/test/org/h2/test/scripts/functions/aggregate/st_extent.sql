-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(V GEOMETRY);
> ok

SELECT ST_EXTENT(V) FROM TEST;
>> null

INSERT INTO TEST VALUES ('POINT(1 1)');
> update count: 1

SELECT ST_EXTENT(V) FROM TEST;
>> POINT (1 1)

INSERT INTO TEST VALUES ('POINT(1 2)'), (NULL), ('POINT(3 1)');
> update count: 3

SELECT ST_EXTENT(V), ST_EXTENT(V) FILTER (WHERE V <> 'POINT(3 1)') FILTERED1,
    ST_EXTENT(V) FILTER (WHERE V <> 'POINT(1 2)') FILTERED2 FROM TEST;
> ST_EXTENT(V)                        FILTERED1             FILTERED2
> ----------------------------------- --------------------- ---------------------
> POLYGON ((1 1, 1 2, 3 2, 3 1, 1 1)) LINESTRING (1 1, 1 2) LINESTRING (1 1, 3 1)
> rows: 1

CREATE SPATIAL INDEX IDX ON TEST(V);
> ok

-- With index
SELECT ST_EXTENT(V) FROM TEST;
>> POLYGON ((1 1, 1 2, 3 2, 3 1, 1 1))

SELECT ST_EXTENT(V) FILTER (WHERE V <> 'POINT(3 1)') FILTERED FROM TEST;
>> LINESTRING (1 1, 1 2)

SELECT ST_EXTENT(V) FROM TEST WHERE V <> 'POINT(3 1)';
>> LINESTRING (1 1, 1 2)

TRUNCATE TABLE TEST;
> ok

-- With index
SELECT ST_EXTENT(V) FROM TEST;
>> null

DROP TABLE TEST;
> ok

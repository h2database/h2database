-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(G GEOMETRY, G_S GEOMETRY(GEOMETRY, 1), P GEOMETRY(POINT), P_S GEOMETRY(POINT, 1),
    PZ1 GEOMETRY(POINT Z), PZ2 GEOMETRY(POINTZ), PZ1_S GEOMETRY(POINT Z, 1), PZ2_S GEOMETRY(POINTZ, 1),
    PM GEOMETRY(POINT M), PZM GEOMETRY(POINT ZM), PZM_S GEOMETRY(POINT ZM, -100),
    LS GEOMETRY(LINESTRING), PG GEOMETRY(POLYGON),
    MP GEOMETRY(MULTIPOINT), MLS GEOMETRY(MULTILINESTRING), MPG GEOMETRY(MULTIPOLYGON),
    GC GEOMETRY(GEOMETRYCOLLECTION));
> ok

INSERT INTO TEST VALUES ('POINT EMPTY', 'SRID=1;POINT EMPTY', 'POINT EMPTY', 'SRID=1;POINT EMPTY',
    'POINT Z EMPTY', 'POINT Z EMPTY', 'SRID=1;POINT Z EMPTY', 'SRID=1;POINTZ EMPTY',
    'POINT M EMPTY', 'POINT ZM EMPTY', 'SRID=-100;POINT ZM EMPTY',
    'LINESTRING EMPTY', 'POLYGON EMPTY',
    'MULTIPOINT EMPTY', 'MULTILINESTRING EMPTY', 'MULTIPOLYGON EMPTY',
    'GEOMETRYCOLLECTION EMPTY');
> update count: 1

SELECT COLUMN_NAME, DATA_TYPE, GEOMETRY_TYPE, GEOMETRY_SRID FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE GEOMETRY_TYPE      GEOMETRY_SRID
> ----------- --------- ------------------ -------------
> G           GEOMETRY  null               null
> G_S         GEOMETRY  null               1
> P           GEOMETRY  POINT              null
> P_S         GEOMETRY  POINT              1
> PZ1         GEOMETRY  POINT Z            null
> PZ2         GEOMETRY  POINT Z            null
> PZ1_S       GEOMETRY  POINT Z            1
> PZ2_S       GEOMETRY  POINT Z            1
> PM          GEOMETRY  POINT M            null
> PZM         GEOMETRY  POINT ZM           null
> PZM_S       GEOMETRY  POINT ZM           -100
> LS          GEOMETRY  LINESTRING         null
> PG          GEOMETRY  POLYGON            null
> MP          GEOMETRY  MULTIPOINT         null
> MLS         GEOMETRY  MULTILINESTRING    null
> MPG         GEOMETRY  MULTIPOLYGON       null
> GC          GEOMETRY  GEOMETRYCOLLECTION null
> rows (ordered): 17

UPDATE TEST SET G = 'SRID=10;LINESTRING EMPTY';
> update count: 1

UPDATE TEST SET GC = 'SRID=8;GEOMETRYCOLLECTION(POINT (1 1))';
> update count: 1

UPDATE TEST SET G_S = 'POINT (1 1)';
> exception DATA_CONVERSION_ERROR_1

UPDATE TEST SET P = 'POINT Z EMPTY';
> exception DATA_CONVERSION_ERROR_1

UPDATE TEST SET P = 'POLYGON EMPTY';
> exception DATA_CONVERSION_ERROR_1

UPDATE TEST SET PZ1 = 'POINT EMPTY';
> exception DATA_CONVERSION_ERROR_1

SELECT * FROM TEST;
> G                        G_S                P           P_S                PZ1           PZ2           PZ1_S                PZ2_S                PM            PZM            PZM_S                    LS               PG            MP               MLS                   MPG                GC
> ------------------------ ------------------ ----------- ------------------ ------------- ------------- -------------------- -------------------- ------------- -------------- ------------------------ ---------------- ------------- ---------------- --------------------- ------------------ ---------------------------------------
> SRID=10;LINESTRING EMPTY SRID=1;POINT EMPTY POINT EMPTY SRID=1;POINT EMPTY POINT Z EMPTY POINT Z EMPTY SRID=1;POINT Z EMPTY SRID=1;POINT Z EMPTY POINT M EMPTY POINT ZM EMPTY SRID=-100;POINT ZM EMPTY LINESTRING EMPTY POLYGON EMPTY MULTIPOINT EMPTY MULTILINESTRING EMPTY MULTIPOLYGON EMPTY SRID=8;GEOMETRYCOLLECTION (POINT (1 1))
> rows: 1

SELECT G FROM TEST WHERE P_S = 'SRID=1;POINT EMPTY';
>> SRID=10;LINESTRING EMPTY

SELECT G FROM TEST WHERE P_S = 'GEOMETRYCOLLECTION Z EMPTY';
> exception DATA_CONVERSION_ERROR_1

CREATE SPATIAL INDEX IDX ON TEST(GC);
> ok

SELECT P FROM TEST WHERE GC = 'SRID=8;GEOMETRYCOLLECTION (POINT (1 1))';
>> POINT EMPTY

SELECT P FROM TEST WHERE GC = 'SRID=8;GEOMETRYCOLLECTION Z (POINT (1 1 1))';
> exception DATA_CONVERSION_ERROR_1

SELECT CAST('POINT EMPTY' AS GEOMETRY(POINT));
>> POINT EMPTY

SELECT CAST('POINT EMPTY' AS GEOMETRY(POINT Z));
> exception DATA_CONVERSION_ERROR_1

SELECT CAST('POINT EMPTY' AS GEOMETRY(POINT, 0));
>> POINT EMPTY

SELECT CAST('POINT EMPTY' AS GEOMETRY(POINT, 1));
> exception DATA_CONVERSION_ERROR_1

SELECT CAST('POINT EMPTY' AS GEOMETRY(POLYGON));
> exception DATA_CONVERSION_ERROR_1

DROP TABLE TEST;
> ok

SELECT CAST('POINT EMPTY'::GEOMETRY AS JSON);
>> null

SELECT CAST('null' FORMAT JSON AS GEOMETRY);
>> POINT EMPTY

SELECT CAST('POINT (1 2)'::GEOMETRY AS JSON);
>> {"type":"Point","coordinates":[1,2]}

SELECT CAST('{"type":"Point","coordinates":[1,2]}' FORMAT JSON AS GEOMETRY);
>> POINT (1 2)

SELECT CAST('POINT Z (1 2 3)'::GEOMETRY AS JSON);
>> {"type":"Point","coordinates":[1,2,3]}

SELECT CAST('{"type":"Point","coordinates":[1,2,3]}' FORMAT JSON AS GEOMETRY);
>> POINT Z (1 2 3)

SELECT CAST('POINT ZM (1 2 3 4)'::GEOMETRY AS JSON);
>> {"type":"Point","coordinates":[1,2,3,4]}

SELECT CAST('{"type":"Point","coordinates":[1,2,3,4]}' FORMAT JSON AS GEOMETRY);
>> POINT ZM (1 2 3 4)

SELECT CAST('POINT M (1 2 4)'::GEOMETRY AS JSON);
> exception DATA_CONVERSION_ERROR_1

SELECT CAST('SRID=4326;POINT (1 2)'::GEOMETRY AS JSON);
>> {"type":"Point","coordinates":[1,2]}

SELECT CAST('{"type":"Point","coordinates":[1,2]}' FORMAT JSON AS GEOMETRY(POINT));
>> POINT (1 2)

SELECT CAST('{"type":"Point","coordinates":[1,2]}' FORMAT JSON AS GEOMETRY(GEOMETRY, 4326));
>> SRID=4326;POINT (1 2)

SELECT CAST('LINESTRING EMPTY'::GEOMETRY AS JSON);
>> {"type":"LineString","coordinates":[]}

SELECT CAST('{"type":"LineString","coordinates":[]}' FORMAT JSON AS GEOMETRY);
>> LINESTRING EMPTY

SELECT CAST('LINESTRING (1 2, 3 4)'::GEOMETRY AS JSON);
>> {"type":"LineString","coordinates":[[1,2],[3,4]]}

SELECT CAST('{"type":"LineString","coordinates":[[1,2],[3,4]]}' FORMAT JSON AS GEOMETRY);
>> LINESTRING (1 2, 3 4)

SELECT CAST('POLYGON EMPTY'::GEOMETRY AS JSON);
>> {"type":"Polygon","coordinates":[]}

SELECT CAST('{"type":"Polygon","coordinates":[]}' FORMAT JSON AS GEOMETRY);
>> POLYGON EMPTY

SELECT CAST('POLYGON ((-1 -2, 10 1, 2 20, -1 -2))'::GEOMETRY AS JSON);
>> {"type":"Polygon","coordinates":[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]]]}

SELECT CAST('{"type":"Polygon","coordinates":[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]]]}' FORMAT JSON AS GEOMETRY);
>> POLYGON ((-1 -2, 10 1, 2 20, -1 -2))

SELECT CAST('POLYGON ((-1 -2, 10 1, 2 20, -1 -2), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5), EMPTY)'::GEOMETRY AS JSON);
>> {"type":"Polygon","coordinates":[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]],[[0.5,0.5],[1,0.5],[1,1],[0.5,0.5]],[]]}

SELECT CAST('{"type":"Polygon","coordinates":[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]],[[0.5,0.5],[1,0.5],[1,1],[0.5,0.5]],[]]}' FORMAT JSON AS GEOMETRY);
>> POLYGON ((-1 -2, 10 1, 2 20, -1 -2), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5), EMPTY)

SELECT CAST('MULTIPOINT EMPTY'::GEOMETRY AS JSON);
>> {"type":"MultiPoint","coordinates":[]}

SELECT CAST('{"type":"MultiPoint","coordinates":[]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOINT EMPTY

SELECT CAST('MULTIPOINT ((1 2))'::GEOMETRY AS JSON);
>> {"type":"MultiPoint","coordinates":[[1,2]]}

SELECT CAST('{"type":"MultiPoint","coordinates":[[1,2]]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOINT ((1 2))

SELECT CAST('MULTIPOINT ((1 2), (3 4))'::GEOMETRY AS JSON);
>> {"type":"MultiPoint","coordinates":[[1,2],[3,4]]}

SELECT CAST('{"type":"MultiPoint","coordinates":[[1,2],[3,4]]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOINT ((1 2), (3 4))

SELECT CAST('MULTIPOINT ((1 0), EMPTY, EMPTY, (2 2))'::GEOMETRY AS JSON);
>> {"type":"MultiPoint","coordinates":[[1,0],null,null,[2,2]]}

SELECT CAST('{"type":"MultiPoint","coordinates":[[1,0],null,null,[2,2]]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOINT ((1 0), EMPTY, EMPTY, (2 2))

SELECT CAST('MULTILINESTRING EMPTY'::GEOMETRY AS JSON);
>> {"type":"MultiLineString","coordinates":[]}

SELECT CAST('{"type":"MultiLineString","coordinates":[]}' FORMAT JSON AS GEOMETRY);
>> MULTILINESTRING EMPTY

SELECT CAST('MULTILINESTRING ((1 2, 3 4, 5 7))'::GEOMETRY AS JSON);
>> {"type":"MultiLineString","coordinates":[[[1,2],[3,4],[5,7]]]}

SELECT CAST('{"type":"MultiLineString","coordinates":[[[1,2],[3,4],[5,7]]]}' FORMAT JSON AS GEOMETRY);
>> MULTILINESTRING ((1 2, 3 4, 5 7))

SELECT CAST('MULTILINESTRING ((1 2, 3 4, 5 7), (-1 -1, 0 0, 2 2, 4 6.01), EMPTY)'::GEOMETRY AS JSON);
>> {"type":"MultiLineString","coordinates":[[[1,2],[3,4],[5,7]],[[-1,-1],[0,0],[2,2],[4,6.01]],[]]}

SELECT CAST('{"type":"MultiLineString","coordinates":[[[1,2],[3,4],[5,7]],[[-1,-1],[0,0],[2,2],[4,6.01]],[]]}' FORMAT JSON AS GEOMETRY);
>> MULTILINESTRING ((1 2, 3 4, 5 7), (-1 -1, 0 0, 2 2, 4 6.01), EMPTY)

SELECT CAST('MULTIPOLYGON EMPTY'::GEOMETRY AS JSON);
>> {"type":"MultiPolygon","coordinates":[]}

SELECT CAST('{"type":"MultiPolygon","coordinates":[]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOLYGON EMPTY

SELECT CAST('MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2)))'::GEOMETRY AS JSON);
>> {"type":"MultiPolygon","coordinates":[[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]]]]}

SELECT CAST('{"type":"MultiPolygon","coordinates":[[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]]]]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2)))

SELECT CAST('MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2)), ((1 2, 2 2, 3 3, 1 2)))'::GEOMETRY AS JSON);
>> {"type":"MultiPolygon","coordinates":[[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]]],[[[1,2],[2,2],[3,3],[1,2]]]]}

SELECT CAST('{"type":"MultiPolygon","coordinates":[[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]]],[[[1,2],[2,2],[3,3],[1,2]]]]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2)), ((1 2, 2 2, 3 3, 1 2)))

SELECT CAST('MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5)))'::GEOMETRY AS JSON);
>> {"type":"MultiPolygon","coordinates":[[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]],[[0.5,0.5],[1,0.5],[1,1],[0.5,0.5]]]]}

SELECT CAST('{"type":"MultiPolygon","coordinates":[[[[-1,-2],[1E1,1],[2,2E1],[-1,-2]],[[0.5,0.5],[1,0.5],[1,1],[0.5,0.5]]]]}' FORMAT JSON AS GEOMETRY);
>> MULTIPOLYGON (((-1 -2, 10 1, 2 20, -1 -2), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5)))

SELECT CAST('GEOMETRYCOLLECTION EMPTY'::GEOMETRY AS JSON);
>> {"type":"GeometryCollection","geometries":[]}

SELECT CAST('{"type":"GeometryCollection","geometries":[]}' FORMAT JSON AS GEOMETRY);
>> GEOMETRYCOLLECTION EMPTY

SELECT CAST('GEOMETRYCOLLECTION (POINT (1 2))'::GEOMETRY AS JSON);
>> {"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}

SELECT CAST('{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}' FORMAT JSON AS GEOMETRY);
>> GEOMETRYCOLLECTION (POINT (1 2))

SELECT CAST('GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 3)), MULTIPOINT ((4 8)))'::GEOMETRY AS JSON);
>> {"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,3]}]},{"type":"MultiPoint","coordinates":[[4,8]]}]}

SELECT CAST('{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,3]}]},{"type":"MultiPoint","coordinates":[[4,8]]}]}' FORMAT JSON AS GEOMETRY);
>> GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 3)), MULTIPOINT ((4 8)))

SELECT CAST('{"type":"Unknown","coordinates":[1,2]}' FORMAT JSON AS GEOMETRY);
> exception DATA_CONVERSION_ERROR_1

EXPLAIN VALUES GEOMETRY 'POINT EMPTY';
>> VALUES (GEOMETRY 'POINT EMPTY')

EXPLAIN VALUES GEOMETRY X'00000000017ff80000000000007ff8000000000000';
>> VALUES (GEOMETRY 'POINT EMPTY')

EXPLAIN VALUES CAST(CAST('POINT EMPTY' AS GEOMETRY) AS VARBINARY);
>> VALUES (CAST(X'00000000017ff80000000000007ff8000000000000' AS BINARY VARYING))

SELECT GEOMETRY X'000000000300000000';
>> POLYGON EMPTY

SELECT GEOMETRY X'00000000030000000100000000';
>> POLYGON EMPTY

SELECT CAST(GEOMETRY 'POLYGON EMPTY' AS VARBINARY);
>> X'000000000300000000'

SELECT CAST(GEOMETRY X'00000000030000000100000000' AS VARBINARY);
>> X'000000000300000000'

VALUES GEOMETRY 'POINT (1 2 3)';
> exception DATA_CONVERSION_ERROR_1

VALUES CAST('SRID=1;POINT(1 1)' AS GEOMETRY(POINT, 1)) UNION VALUES CAST('SRID=1;POINT Z(1 1 1)' AS GEOMETRY(POINT Z, 1));
> C1
> ----------------------
> SRID=1;POINT (1 1)
> SRID=1;POINT Z (1 1 1)
> rows: 2

VALUES CAST('SRID=1;POINT(1 1)' AS GEOMETRY(POINT, 1)) UNION VALUES CAST('SRID=2;POINT Z(1 1 1)' AS GEOMETRY(POINT Z, 2));
> exception TYPES_ARE_NOT_COMPARABLE_2

VALUES CAST('SRID=1;POINT(1 1)' AS GEOMETRY(POINT, 1)) UNION VALUES CAST('SRID=1;POINT (2 2)' AS GEOMETRY(POINT, 1));
> C1
> ------------------
> SRID=1;POINT (1 1)
> SRID=1;POINT (2 2)
> rows: 2

VALUES CAST('POINT(1 1)' AS GEOMETRY(GEOMETRY, 0)) UNION VALUES CAST('POINT (2 2)' AS GEOMETRY);
> C1
> -----------
> POINT (1 1)
> POINT (2 2)
> rows: 2

VALUES CAST('POINT(1 1)' AS GEOMETRY(POINT)) UNION VALUES CAST('POINT Z (1 1 1)' AS GEOMETRY(POINT Z));
> C1
> ---------------
> POINT (1 1)
> POINT Z (1 1 1)
> rows: 2

VALUES CAST('SRID=1;POINT(1 1)' AS GEOMETRY(POINT, 1)) UNION VALUES NULL;
> C1
> ------------------
> SRID=1;POINT (1 1)
> null
> rows: 2

VALUES NULL UNION VALUES CAST('POINT(1 1)' AS GEOMETRY(POINT));
> C1
> -----------
> POINT (1 1)
> null
> rows: 2

VALUES CAST(GEOMETRY 'POINT EMPTY' AS GEOMETRY)
UNION
VALUES CAST(GEOMETRY 'SRID=10;POINT EMPTY' AS GEOMETRY(GEOMETRY, 10));
> C1
> -------------------
> POINT EMPTY
> SRID=10;POINT EMPTY
> rows: 2

VALUES CAST(GEOMETRY 'POINT EMPTY' AS GEOMETRY(POINT))
UNION
VALUES CAST(GEOMETRY 'SRID=10;POINT EMPTY' AS GEOMETRY(POINT, 10));
> C1
> -------------------
> POINT EMPTY
> SRID=10;POINT EMPTY
> rows: 2

VALUES CAST(GEOMETRY 'POINT EMPTY' AS GEOMETRY(POINT))
UNION
VALUES CAST(GEOMETRY 'SRID=10;MULTIPOINT EMPTY' AS GEOMETRY(MULTIPOINT, 10));
> C1
> ------------------------
> POINT EMPTY
> SRID=10;MULTIPOINT EMPTY
> rows: 2

DROP TABLE IF EXISTS outdegree;
DROP TABLE IF EXISTS dangling;
DROP TABLE IF EXISTS pr0;
DROP TABLE IF EXISTS pr1;

SELECT count(*) AS numVertices FROM v;

CREATE TABLE outdegree AS
    SELECT source AS id, count(target) AS value
    FROM e
    GROUP BY source
;

CREATE TABLE dangling AS
    SELECT v.id AS id
    FROM v
    WHERE NOT EXISTS (SELECT 1 FROM e WHERE e.source = v.id)
;

CREATE TABLE pr0 AS
    SELECT
        id,
        CAST(1 AS FLOAT) / (SELECT count(*) AS numVertices FROM v) AS value
    FROM v;

-- redistributed from dangling
SELECT
    0.85 / (SELECT count(*) AS numVertices FROM v)
    *
    (SELECT sum(value) FROM dangling NATURAL JOIN pr0)
    AS redistributed
;

-- importance
CREATE TABLE pr1 AS
    SELECT
        pr0.id,
        (SELECT (1 - 0.85) / (SELECT count(*) AS numVertices FROM v))
        +
        0.85 * CASE WHEN (sum(outdegree.value)) IS NULL THEN 0 ELSE
        sum(
            (SELECT value FROM pr0 WHERE id = e.source) /
            outdegree.value
        )
        END
        + 
        (SELECT 0.85 / (SELECT count(*) AS numVertices FROM v) * (SELECT sum(value) FROM dangling NATURAL JOIN pr0))
        AS value
    FROM pr0
    LEFT JOIN e
        ON e.target = pr0.id
    LEFT JOIN outdegree
        ON e.source = outdegree.id
    GROUP BY pr0.id, pr0.value
    ORDER BY pr0.id ASC
    ;

SELECT * FROM pr1;
SELECT sum(value) FROM pr1;

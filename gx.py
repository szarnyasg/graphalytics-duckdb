import duckdb

con = duckdb.connect(database=':memory:', read_only=False)

con.execute("CREATE TABLE v(id INTEGER)")
con.execute("CREATE TABLE e(source INTEGER, target INTEGER, value DOUBLE)")
con.execute("CREATE TABLE cdlp0(id INTEGER, label INTEGER)")
con.execute("CREATE TABLE cdlp1(id INTEGER, label INTEGER)")
con.execute("CREATE TABLE cdlp2(id INTEGER, label INTEGER)")
# TODO: more CDLP tables and just drop them

graph = "/home/szarnyasg/graphs/example-undirected"
undirected = True

con.execute(f"COPY v (id) FROM '{graph}.v' (DELIMITER ' ', FORMAT csv);")

con.execute(f"COPY e (source, target, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv);")
if undirected:
    con.execute(f"COPY e (target, source, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv);")

# LCC
print("LCC")
con.execute("""
    SELECT e1.source, CAST(count(*) AS float) / (count(DISTINCT e2.target)*(count(DISTINCT e2.target)-1))
    FROM e e1, e e2, e e3
    WHERE e1.target = e2.source
      AND e2.target = e3.source
      AND e3.target = e1.source
    GROUP BY e1.source
    ORDER BY e1.source ASC
    """)
results = con.fetchall()
for result in results:
    print(result)

# CDLP
print("CDLP // 1")
con.execute("""
    INSERT INTO cdlp0
    SELECT id, id
    FROM v;
    """)
con.execute("""
    SELECT * FROM cdlp0
    """)
results = con.fetchall()
for result in results:
    print(result)

#for i in range(0, 5):
print("CDLP // 2")
print("=====================")
con.execute("""
    SELECT e.source AS id, cdlp0.label AS label, count(*) AS count
        FROM e, cdlp0
        WHERE cdlp0.id = e.target
        GROUP BY e.source, cdlp0.label
    """)
results = con.fetchall()
for result in results:
    print(result)
print("=====================")
con.execute("""
    INSERT INTO cdlp1
    SELECT DISTINCT
        id, min(label) OVER (PARTITION BY id ORDER BY id ASC)
    FROM (
        SELECT e.source AS id, cdlp0.label AS label, count(*) AS count
        FROM e, cdlp0
        WHERE cdlp0.id = e.target
        GROUP BY e.source, cdlp0.label
    ) AS x
    """)
con.execute("""
    SELECT *
    FROM cdlp1
    """)
results = con.fetchall()
for result in results:
    print(result)
print("=====================")

con.execute("""
    SELECT e.source AS id, cdlp1.label AS label, count(*) AS count
    FROM e, cdlp1
    WHERE cdlp1.id = e.target
    GROUP BY e.source, cdlp1.label
    ORDER BY e.source
    """)
results = con.fetchall()
for result in results:
    print(result)

print("=====================")
# TODO flatten
con.execute("""
    SELECT
        id, label FROM
        (
            SELECT
                id, label, count, ROW_NUMBER() OVER (PARTITION BY id ORDER BY count DESC, label ASC) AS seqnum
            FROM (
                SELECT e.source AS id, cdlp1.label AS label, count(*) AS count
                FROM e, cdlp1
                WHERE cdlp1.id = e.target
                GROUP BY e.source, cdlp1.label
            ) AS x
        ) y
    WHERE seqnum = 1
    """)
results = con.fetchall()
for result in results:
    print(result)

print("=====================")
con.execute("""
    SELECT id, label FROM (
        SELECT
            e.source AS id,
            cdlp1.label AS label,
            ROW_NUMBER() OVER (PARTITION BY e.source ORDER BY count(*) DESC, cdlp1.label ASC) AS seqnum
        FROM e, cdlp1
        WHERE cdlp1.id = e.target
        GROUP BY
            e.source,
            cdlp1.label
        ) most_frequent_labels
    WHERE seqnum = 1
    """)
results = con.fetchall()
for result in results:
    print(result)

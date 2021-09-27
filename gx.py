import duckdb

con = duckdb.connect(database=':memory:', read_only=False)

bfs_source = 2
sssp_source = 2
pr_d = 0.85
pr_iterations = 2
cdlp_iterations = 2

con.execute("CREATE TABLE v(id INTEGER)")
con.execute("CREATE TABLE e(source INTEGER, target INTEGER, value DOUBLE)")
# maybe do tic-toc style interations?
for i in range(0, cdlp_iterations+1):
    con.execute(f"CREATE TABLE cdlp{i}(id INTEGER, label INTEGER)")

#graph = "/home/szarnyasg/graphs/example-undirected"
#undirected = True
graph = "/home/szarnyasg/graphs/example-directed"
undirected = False

con.execute(f"COPY v (id) FROM '{graph}.v' (DELIMITER ' ', FORMAT csv);")

con.execute(f"COPY e (source, target, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv);")
if undirected:
    con.execute(f"COPY e (target, source, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv);")

# LCC
print("====================")
print("LCC")
print("====================")
con.execute("""CREATE VIEW neighbors AS (
    SELECT e.source AS vertex, e.target AS neighbor
    FROM e
    UNION
    SELECT e.target AS vertex, e.source AS neighbor
    FROM e
    )
""")
con.execute("""
SELECT
  id,
  CASE WHEN tri = 0 THEN 0.0 ELSE (CAST(tri AS float) / (deg*(deg-1))) END AS value
FROM (
    SELECT
    v.id AS id,
    (SELECT count(*) FROM neighbors WHERE neighbors.vertex = v.id) AS deg,
    (SELECT count(*)
    FROM neighbors n1
    JOIN neighbors n2
      ON n1.vertex = n2.vertex
    JOIN e e3
      ON e3.source = n1.neighbor
     AND e3.target = n2.neighbor
    WHERE n1.vertex = v.id
    ) AS tri
    FROM v
    ORDER BY v.id ASC
) s
""")
results = con.fetchall()
for result in results:
    print(result)

# CDLP
print()
print("====================")
print("CDLP")
print("====================")
con.execute("""
    INSERT INTO cdlp0
    SELECT id, id
    FROM v;
    """)

# We select the minimum mode value (the smallest one from the most frequent labels).
# We use the cdlp{i} table to compute cdlp{i+1}, then throw away the cdlp{i} table.
for i in range(0, cdlp_iterations):
    con.execute(f"""
    INSERT INTO cdlp{i+1}
    SELECT id, label FROM (
        SELECT
            e.source AS id,
            cdlp{i}.label AS label,
            ROW_NUMBER() OVER (PARTITION BY e.source ORDER BY count(*) DESC, cdlp{i}.label ASC) AS seqnum
        FROM e, cdlp{i}
        WHERE cdlp{i}.id = e.target
        GROUP BY
            e.source,
            cdlp{i}.label
        ) most_frequent_labels
    WHERE seqnum = 1
    """)
    con.execute(f"DROP TABLE cdlp{i}")

con.execute(f"SELECT * FROM cdlp{cdlp_iterations}")
results = con.fetchall()
for result in results:
    print(result)

# PR
print("====================")
print("PR")
print("====================")
# should be relatively straightforward to implement using pr_iterations join/aggregate queries

# SSSP
print("====================")
print("SSSP")
print("====================")
# http://aprogrammerwrites.eu/?p=1391
# http://aprogrammerwrites.eu/?p=1415
# https://learnsql.com/blog/get-to-know-the-power-of-sql-recursive-queries/

# BFS
print("====================")
print("BFS")
print("====================")
# use recursive SQL or a sequence of joins?

# WCC
print("====================")
print("WCC")
print("====================")
# check out "In-database connected component analysis", https://arxiv.org/pdf/1802.09478.pdf

import duckdb

con = duckdb.connect(database=':memory:')
#con = duckdb.connect(database='test.duckdb')

directed = True

bfs_source = 1
sssp_source = 2
pr_d = 0.85
pr_iterations = 2
cdlp_iterations = 2

## set data set
if directed:
    graph = "/home/szarnyasg/graphs/example-directed"
else:
    graph = "/home/szarnyasg/graphs/example-undirected"

## graph tables
con.execute("CREATE TABLE v (id INTEGER)")
con.execute("CREATE TABLE e (source INTEGER, target INTEGER, value DOUBLE)")

## loading
con.execute(f"COPY v (id) FROM '{graph}.v' (DELIMITER ' ', FORMAT csv)")
con.execute(f"COPY e (source, target, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv)")

## auxiliary tables
### CDLP
for i in range(0, cdlp_iterations+1):
    con.execute(f"CREATE TABLE cdlp{i} (id INTEGER, label INTEGER)")
### PR
for i in range(0, pr_iterations+1):
    con.execute(f"CREATE TABLE pr{i} (id INTEGER, value DOUBLE)")
### BFS
con.execute("CREATE TABLE frontier(id INTEGER)")
con.execute("CREATE TABLE next(id INTEGER)")
con.execute("CREATE TABLE seen(id INTEGER, level INTEGER)")



# create undirected variant:
# - for directed graphs, it is an actual table
# - for undirected ones, it is just a view on table e
if directed:
    con.execute(f"CREATE TABLE u (target INTEGER, source INTEGER, value INTEGER)")
    con.execute(f"COPY u (target, source, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv)")
    con.execute(f"COPY u (source, target, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv)")
else:
    con.execute(f"COPY e (target, source, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv)")
    con.execute(f"CREATE VIEW u AS SELECT source, target, value FROM e")

# LCC
print("========================================")
print("LCC")
print("========================================")
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
        (
            SELECT count(*)
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
print("========================================")
print("CDLP")
print("========================================")
con.execute("""
    INSERT INTO cdlp0
    SELECT id, id
    FROM v
    """)

# We select the minimum mode value (the smallest one from the most frequent labels).
# We use the cdlp{i-1} table to compute cdlp{i}, then throw away the cdlp{i-1} table.
for i in range(1, cdlp_iterations+1):
    con.execute(f"""
    INSERT INTO cdlp{i}
    SELECT id, label FROM (
        SELECT
            u.source AS id,
            cdlp{i-1}.label AS label,
            ROW_NUMBER() OVER (PARTITION BY u.source ORDER BY count(*) DESC, cdlp{i-1}.label ASC) AS seqnum
        FROM u
        LEFT JOIN cdlp{i-1}
          ON cdlp{i-1}.id = u.target
        GROUP BY
            u.source,
            cdlp{i-1}.label
        ) most_frequent_labels
    WHERE seqnum = 1
    ORDER BY id
    """)
    con.execute(f"DROP TABLE cdlp{i-1}")

con.execute(f"SELECT * FROM cdlp{cdlp_iterations}")
results = con.fetchall()
for result in results:
    print(result)

# TODO: CDLP directed is incorrect

# PR
print("========================================")
print("PR")
print("========================================")

results = con.execute("SELECT count(*) AS n FROM v")
pr_n = con.fetchone()[0]

pr_teleport = (1-pr_d)/pr_n
pr_dangling_redistribution_factor = pr_d/pr_n

con.execute(f"""
    CREATE TABLE dangling AS
    SELECT id FROM v WHERE NOT EXISTS (SELECT 1 FROM e WHERE source = id)
    """)

con.execute(f"""
    CREATE TABLE e_with_source_outdegrees AS
    SELECT e1.source AS source, e1.target AS target, count(e2.target) AS outdegree
    FROM e e1
    JOIN e e2
      ON e1.source = e2.source
    GROUP BY e1.source, e1.target
    """)

# initialize PR_0
con.execute(f"""
    INSERT INTO pr0
    SELECT id, 1.0/{pr_n} FROM v
    """)

# compute PR_1, ..., PR_#iterations
for i in range(1, pr_iterations+1):
    con.execute(f"""
    INSERT INTO pr{i}
    SELECT
        v.id AS id,
        {pr_teleport} +
        {pr_d} * coalesce(sum(pr{i-1}.value / e_with_source_outdegrees.outdegree), 0) +
        {pr_dangling_redistribution_factor} * (SELECT coalesce(sum(pr{i-1}.value), 0) FROM pr{i-1} JOIN dangling ON pr{i-1}.id = dangling.id)
            AS value
    FROM v
    LEFT JOIN e_with_source_outdegrees
           ON e_with_source_outdegrees.target = v.id
    LEFT JOIN pr{i-1}
           ON pr{i-1}.id = e_with_source_outdegrees.source
    GROUP BY v.id
    """)
    con.execute(f"DROP TABLE pr{i-1}")

con.execute(f"SELECT * FROM pr{pr_iterations}")
results = con.fetchall()
for result in results:
    print(result)

# # SSSP
# print("========================================")
# print("SSSP")
# print("========================================")
# # http://aprogrammerwrites.eu/?p=1391
# # http://aprogrammerwrites.eu/?p=1415
# # https://learnsql.com/blog/get-to-know-the-power-of-sql-recursive-queries/

# # BFS
print("========================================")
print("BFS")
print("========================================")

# initial node
level = 0
con.execute(f"INSERT INTO next VALUES ({bfs_source})")
con.execute(f"INSERT INTO seen (SELECT id, {level} FROM next)")
con.execute(f"DELETE FROM frontier")
con.execute(f"INSERT INTO frontier (SELECT * FROM next)")
con.execute(f"DELETE FROM next")

while True:
    level = level + 1

    con.execute(f"""
        INSERT INTO next
        (SELECT DISTINCT e.target
        FROM frontier
        JOIN e
        ON e.source = frontier.id
        WHERE NOT EXISTS (SELECT 1 FROM seen WHERE id = e.target))
        """)

    con.execute(f"SELECT count(id) AS count FROM next")
    count = con.fetchone()[0]
    if count == 0:
        break

    con.execute(f"INSERT INTO seen (SELECT id, {level} FROM next)")
    con.execute(f"DELETE FROM frontier")
    con.execute(f"INSERT INTO frontier (SELECT * FROM next)")
    con.execute(f"DELETE FROM next")

con.execute(f"SELECT * FROM seen")
results = con.fetchall()
for result in results:
    print(result)



# # WCC
# print("========================================")
# print("WCC")
# print("========================================")
# # check out "In-database connected component analysis", https://arxiv.org/pdf/1802.09478.pdf

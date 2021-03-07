import duckdb

con = duckdb.connect(database=':memory:', read_only=False)

bfs_source = 2
sssp_source = 2
pr_d = 0.85
pr_iterations = 2
cdlp_iterations = 2

con.execute("CREATE TABLE v(id INTEGER)")
con.execute("CREATE TABLE e(source INTEGER, target INTEGER, value DOUBLE)")
for i in range(0, cdlp_iterations+1):
    con.execute(f"CREATE TABLE cdlp{i}(id INTEGER, label INTEGER)")

graph = "/home/szarnyasg/graphs/example-undirected"
undirected = True

con.execute(f"COPY v (id) FROM '{graph}.v' (DELIMITER ' ', FORMAT csv);")

con.execute(f"COPY e (source, target, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv);")
if undirected:
    con.execute(f"COPY e (target, source, value) FROM '{graph}.e' (DELIMITER ' ', FORMAT csv);")

# LCC
print("====================")
print("LCC")
print("====================")
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
print()
print("====================")
print("CDLP")
print("====================")
con.execute("""
    INSERT INTO cdlp0
    SELECT id, id
    FROM v;
    """)

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

# SSSP
print("====================")
print("SSSP")
print("====================")

# BFS
print("====================")
print("BFS")
print("====================")

# WCC
print("====================")
print("WCC")
print("====================")

import duckdb

con = duckdb.connect(database=':memory:', read_only=False)

con.execute("CREATE TABLE e(source INTEGER, target INTEGER, value DOUBLE)")

edge_file = "/home/szarnyasg/graphs/example-undirected.e"

con.execute(f"COPY e (source, target, value) FROM '{edge_file}' (DELIMITER ' ', FORMAT csv);")
con.execute(f"COPY e (target, source, value) FROM '{edge_file}' (DELIMITER ' ', FORMAT csv);")



# triangle count
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

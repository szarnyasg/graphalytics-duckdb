
directed = False

pr_d = 0.85
pr_iterations = 2
cdlp_iterations = 2




## set data set
if directed:
    graph = "graphs/example-directed"
    bfs_source = 1
    sssp_source = 1
else:
    graph = "graphs/example-undirected"
    bfs_source = 2
    sssp_source = 2

## TODO: handle unweighted edges

init_tables(con, graph, directed)

bfs(con, bfs_source)
pr(con, pr_iterations)
sssp(con, sssp_source)
wcc(con)
lcc(con)
cdlp(con, cdlp_iterations)



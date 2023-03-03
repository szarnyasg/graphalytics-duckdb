#!/usr/bin/env python3

import duckdb
import argparse
import random
import os

def relabel(con, graph, input_vertex_path, input_edge_path, output_path, directed, weighted):
    print("Loading...")
    ## graph tables
    con.execute(f"SET experimental_parallel_csv=true")
    con.execute(f"CREATE TABLE v (id INTEGER)")
    if weighted:
        weight_attribute_without_type = ", weight"
        weight_attribute_with_type = ", weight DOUBLE"
    else:
        weight_attribute_without_type = ""
        weight_attribute_with_type = ""
 
    con.execute(f"CREATE TABLE e (source INTEGER, target INTEGER{weight_attribute_with_type})")
 
    ## loading
    con.execute(f"COPY v (id) FROM '{input_vertex_path}' (DELIMITER ' ', FORMAT csv)")
    con.execute(f"COPY e (source, target{weight_attribute_without_type}) FROM '{input_edge_path}' (DELIMITER ' ', FORMAT csv)")

    # create undirected variant:
    # - for directed graphs, it is an actual table
    # - for undirected ones, it is just a view on table e
    if not directed:
        # copy reverse edges to 'e'
        con.execute(f"INSERT INTO e SELECT target, source{weight_attribute_without_type} FROM e")

    print("Relabelling...")
    con.execute(f"""
        CREATE VIEW e_relabelled AS
        SELECT source_vertex.rowid AS source, target_vertex.rowid AS target, weight
        FROM e
        JOIN v source_vertex ON source_vertex.id = e.source
        JOIN v target_vertex ON target_vertex.id = e.target
        """)

    if directed:
        matrix_type = 'symmetric'
    else:
        matrix_type = 'general'
    
    if weighted:
        element_type = 'real'
        filename_postfix = 'fp64'
    else:
        element_type = 'bool'
        filename_postfix = 'bool'

    print("Serializing vertex mapping...")
    con.execute(f"""
        COPY (
            SELECT v.id
            FROM v
            ORDER BY v.rowid
        )
        TO '{output_path}/{graph}.vtx'
        WITH (HEADER false)
        """)

    print("Serializing edge mapping...")
    con.execute(f"""
        COPY (
                SELECT '%%MatrixMarket matrix coordinate {element_type} {matrix_type}' AS s
            UNION ALL
                SELECT (SELECT count(*) FROM v) || ' ' || (SELECT count(*) FROM v) || ' ' || (SELECT count(*) FROM e_relabelled)
            UNION ALL
            (
                SELECT source || ' ' || target || ' ' || weight
                FROM e_relabelled
                ORDER BY source, target
            )
        )
        TO '{output_path}/{graph}-{filename_postfix}.mtx'
        WITH (HEADER false)
        """)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--graph-name',          type=str,  required=True)
    parser.add_argument('--input-vertex-path',   type=str,  required=True)
    parser.add_argument('--input-edge-path',     type=str,  required=True)
    parser.add_argument('--output-path',         type=str,  required=True)
    parser.add_argument('--weighted',            type=bool, required=True)
    parser.add_argument('--directed',            type=bool, required=True)
    parser.add_argument('--use_disk', action='store_true',  required=False)
    args = parser.parse_args()

    if args.use_disk:
        dbfile = "test.duckdb"
        if os.path.exists(dbfile):
            os.remove(dbfile)
        con = duckdb.connect(database=dbfile)
    else:
        con = duckdb.connect(database=":memory:")

    relabel(con, \
            args.graph_name, args.input_vertex_path, args.input_edge_path, \
            args.output_path, args.directed, args.weighted)


if __name__ == "__main__":
    main()

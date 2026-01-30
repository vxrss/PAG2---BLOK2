from neo4j import GraphDatabase
from pyproj import Transformer


URI = "bolt://localhost:7687"
AUTH = ("neo4j", "adminadmin")

driver = GraphDatabase.driver(URI, auth=AUTH)

def run(q, p=None):
    with driver.session() as s:
        return list(s.run(q, p or {}))


_to_1992 = Transformer.from_crs("EPSG:4326", "EPSG:2180", always_xy=True)
_to_wgs = Transformer.from_crs("EPSG:2180", "EPSG:4326", always_xy=True)


def wgs84_to_1992(lon, lat):
    return _to_1992.transform(lon, lat)

def to_wgs(x, y):
    return _to_wgs.transform(x, y)

def init_gds():
    run("CALL gds.graph.drop('roads_length', false)")
    run("CALL gds.graph.drop('roads_time', false)")

    run("""
    CALL gds.graph.project(
        'roads_length',
        'Node',
        { ROAD: { orientation:'UNDIRECTED', properties:'length' } }
    )
    """)

    run("""
    CALL gds.graph.project(
        'roads_time',
        'Node',
        { ROAD: { orientation:'UNDIRECTED', properties:'time' } },
        { nodeProperties:['x_astar','y_astar'] }
    )
    """)

def find_nearest_node(x, y, max_dist=150):
    r = run("""
    MATCH (n:Node)
    WITH n, sqrt((n.x-$x)^2 + (n.y-$y)^2) AS d
    WHERE d < $max
    RETURN id(n) AS id
    ORDER BY d
    LIMIT 1
    """, {"x": x, "y": y, "max": max_dist})

    if not r:
        raise RuntimeError("Brak drogi w pobliżu punktu")

    return r[0]["id"]

def dijkstra_length(s, t):
    r = run("""
    MATCH (a:Node),(b:Node)
    WHERE id(a)=$s AND id(b)=$t
    CALL gds.shortestPath.dijkstra.stream(
        'roads_length',
        { sourceNode:a, targetNode:b, relationshipWeightProperty:'length' }
    )
    YIELD nodeIds, totalCost
    RETURN nodeIds, totalCost
    """, {"s": s, "t": t})

    return (r[0]["nodeIds"], r[0]["totalCost"]) if r else ([], None)

def astar_time(s, t):
    r = run("""
    MATCH (a:Node),(b:Node)
    WHERE id(a)=$s AND id(b)=$t
    CALL gds.shortestPath.astar.stream(
        'roads_time',
        {
            sourceNode:a,
            targetNode:b,
            longitudeProperty:'x_astar',
            latitudeProperty:'y_astar',
            relationshipWeightProperty:'time'
        }
    )
    YIELD nodeIds, totalCost
    RETURN nodeIds, totalCost
    """, {"s": s, "t": t})

    return (r[0]["nodeIds"], r[0]["totalCost"]) if r else ([], None)

def get_coords(node_ids):
    rows = run("""
    UNWIND $ids AS i
    MATCH (n:Node)
    WHERE id(n)=i
    RETURN n.x AS x, n.y AS y
    """, {"ids": node_ids})

    coords = []
    for r in rows:
        lon, lat = to_wgs(r["x"], r["y"])
        coords.append([lat, lon])

    return coords


def get_path_stats(node_ids):
    r = run("""
    UNWIND range(0, size($ids)-2) AS i
    MATCH (a:Node)-[r:ROAD]-(b:Node)
    WHERE id(a)=$ids[i] AND id(b)=$ids[i+1]
    RETURN sum(r.length) AS len
    """, {"ids": node_ids})

    return float(r[0]["len"] or 0.0)
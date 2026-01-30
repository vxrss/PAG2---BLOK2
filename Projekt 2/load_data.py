import arcpy
import math
import csv
import os
import time
from neo4j import GraphDatabase

arcpy.env.overwriteOutput = True


PATH = r"dane\L4_1_BDOT10k__OT_SKJZ_L.shp"
OUT_DIR = r"wyniki"

TOLERANCJA = 0.5

NEO4J_URI = "bolt://127.0.0.1:7687"
NEO4J_AUTH = ("neo4j", "adminadmin")


V_MAX_KMH = 140
V_MAX = V_MAX_KMH / 3.6  # m/s

SPEED_MAP = {
    "A": 140,
    "S": 120,
    "GP": 100,
    "G": 90,
    "Z": 50,
    "L": 50,
    "D": 50,
    "I": 50
}


TIMINGS = {
    "vertex": 0.0,
    "total": 0.0
}


def load_data(path: str):
    fields = ["Shape@", "klasaDrogi"]
    data = []

    with arcpy.da.SearchCursor(path, fields) as cur:
        for geom, klasa in cur:
            if not geom or not geom.firstPoint or not geom.lastPoint:
                continue

            data.append({
                "start": (geom.firstPoint.X, geom.firstPoint.Y),
                "end": (geom.lastPoint.X, geom.lastPoint.Y),
                "length": float(geom.length),
                "klasa": klasa
            })

    return data


def find_or_create_vertex(point, vertices):
    t0 = time.perf_counter()

    for vid, (x, y) in vertices.items():
        if math.hypot(x - point[0], y - point[1]) <= TOLERANCJA:
            TIMINGS["vertex"] += time.perf_counter() - t0
            return vid

    new_id = len(vertices) + 1
    vertices[new_id] = point

    TIMINGS["vertex"] += time.perf_counter() - t0
    return new_id



t_start = time.perf_counter()


data = load_data(PATH)

vertices = {}
edges = []


for edge_id, rec in enumerate(data, start=1):
    v_from = find_or_create_vertex(rec["start"], vertices)
    v_to = find_or_create_vertex(rec["end"], vertices)

    speed_kmh = SPEED_MAP.get(rec["klasa"], 50)
    speed_m_s = speed_kmh / 3.6
    travel_time = rec["length"] / speed_m_s

    edges.append([
        edge_id,
        v_from,
        v_to,
        rec["klasa"],
        rec["length"],
        travel_time
    ])

TIMINGS["total"] = time.perf_counter() - t_start


os.makedirs(OUT_DIR, exist_ok=True)

vertices_csv = os.path.join(OUT_DIR, "vertices.csv")
edges_csv = os.path.join(OUT_DIR, "edges.csv")


with open(vertices_csv, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["vertex_id", "x", "y", "x_astar", "y_astar"])
    for vid, (x, y) in vertices.items():
        w.writerow([
            vid,
            x,
            y,
            x / V_MAX,
            y / V_MAX
        ])


with open(edges_csv, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "edge_id",
        "from_vertex",
        "to_vertex",
        "klasaDrogi",
        "length_m",
        "time_s"
    ])
    for e in edges:
        w.writerow(e)


driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

def run(query, params=None):
    with driver.session() as session:
        session.run(query, params or {})


run("MATCH (n) DETACH DELETE n")


with open(vertices_csv, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        run("""
        CREATE (:Node {
            id: $id,
            x: $x,
            y: $y,
            x_astar: $xa,
            y_astar: $ya
        })
        """, {
            "id": int(r["vertex_id"]),
            "x": float(r["x"]),
            "y": float(r["y"]),
            "xa": float(r["x_astar"]),
            "ya": float(r["y_astar"])
        })

run("""
CREATE CONSTRAINT node_id IF NOT EXISTS
FOR (n:Node) REQUIRE n.id IS UNIQUE
""")


with open(edges_csv, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
        run("""
        MATCH (a:Node {id:$from})
        MATCH (b:Node {id:$to})
        CREATE (a)-[:ROAD {
            length: $length,
            time: $time,
            class: $cls
        }]->(b)
        CREATE (b)-[:ROAD {
            length: $length,
            time: $time,
            class: $cls
        }]->(a)
        """, {
            "from": int(r["from_vertex"]),
            "to": int(r["to_vertex"]),
            "length": float(r["length_m"]),
            "time": float(r["time_s"]),
            "cls": r["klasaDrogi"]
        })


print("▶ Inicjalizacja GDS...")

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

driver.close()




import sys, os, json, time
from urllib.parse import parse_qs

from PyQt5.QtCore import QUrl, pyqtSlot
from PyQt5.QtWidgets import (
    QApplication, QWidget, QHBoxLayout, QVBoxLayout,
    QLabel, QTextEdit
)
from PyQt5.QtWebEngineWidgets import QWebEngineView, QWebEnginePage

from neo import (
    wgs84_to_1992,
    find_nearest_node,
    dijkstra_length,
    astar_time,
    get_coords,
    get_path_stats
)


class MapPage(QWebEnginePage):
    def __init__(self, parent, callback):
        super().__init__(parent)
        self.callback = callback

    def acceptNavigationRequest(self, url, *_):
        if url.scheme() == "route":
            q = parse_qs(url.query())
            self.callback(
                float(q["sy"][0]), float(q["sx"][0]),
                float(q["ey"][0]), float(q["ex"][0])
            )
            return False
        return True



class App(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Routing Neo4j – Toruń")
        self.resize(1200, 700)

        main = QHBoxLayout(self)


        panel = QVBoxLayout()
        panel.addWidget(QLabel("<b>Porównanie tras</b>"))

        self.info = QTextEdit()
        self.info.setReadOnly(True)
        panel.addWidget(self.info)

        panel.addStretch()
        main.addLayout(panel, 1)


        self.view = QWebEngineView()
        self.page = MapPage(self.view, self.compute_route)
        self.view.setPage(self.page)

        self.view.load(QUrl.fromLocalFile(
            os.path.abspath("map.html")
        ))

        main.addWidget(self.view, 4)


    @pyqtSlot(float, float, float, float)
    def compute_route(self, sy, sx, ey, ex):
        try:

            sx92, sy92 = wgs84_to_1992(sx, sy)
            ex92, ey92 = wgs84_to_1992(ex, ey)

            s = find_nearest_node(sx92, sy92)
            t = find_nearest_node(ex92, ey92)

            if s == t:
                raise RuntimeError("Start i koniec na tym samym węźle")


            t0 = time.perf_counter()
            nodes_d, _ = dijkstra_length(s, t)
            time_d = (time.perf_counter() - t0) * 1000

            if not nodes_d:
                raise RuntimeError("Brak trasy (Dijkstra)")

            len_d = get_path_stats(nodes_d)
            coords_d = get_coords(nodes_d)


            t0 = time.perf_counter()
            nodes_a, travel_time = astar_time(s, t)
            time_a = (time.perf_counter() - t0) * 1000

            if not nodes_a:
                raise RuntimeError("Brak trasy (A*)")

            len_a = get_path_stats(nodes_a)
            coords_a = get_coords(nodes_a)


            self.view.page().runJavaScript("clearRoutes();")


            self.view.page().runJavaScript(
                f"drawRoute({json.dumps(coords_d)}, 'blue');"
            )
            self.view.page().runJavaScript(
                f"drawRoute({json.dumps(coords_a)}, 'red');"
            )


            self.info.setText(
                "\n".join([
                    "Dijkstra – najkrótsza (Trasa niebieska)",
                    f"  Długość: {len_d/1000:.2f} km",
                    f"  Czas obliczeń: {time_d:.1f} ms",
                    "",
                    "A* – najszybsza (Trasa czerwona)",
                    f"  Długość: {len_a/1000:.2f} km",
                    f"  Czas przejazdu: {travel_time/60:.1f} min",
                    f"  Czas obliczeń: {time_a:.1f} ms",
                ])
            )

        except Exception as e:
            self.info.setText(f" {e}")



if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = App()
    w.show()
    sys.exit(app.exec_())

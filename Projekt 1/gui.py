import sys
import json
import pandas as pd
from pymongo import MongoClient
import redis

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QGridLayout, QLabel,
    QComboBox, QPushButton, QCheckBox,
    QMessageBox, QFrame, QTabWidget,
    QTableWidget, QTableWidgetItem
)
from PyQt5.QtCore import Qt

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

PARAM_INFO = {
    "temperatura": ("Temperatura", "°C", "tab:red"),
    "wiatr": ("Wiatr", "m/s", "tab:blue"),
    "opad": ("Opad", "mm", "tab:green"),
}

def connect_mongo():
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        client.server_info()
        print("[OK] MongoDB połączone")
        return client
    except Exception as e:
        print("[ERROR] MongoDB:", e)
        return None


def connect_redis():
    try:
        r = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
            socket_connect_timeout=2
        )
        r.ping()
        print("[OK] Redis połączony")
        return r
    except Exception as e:
        print("[ERROR] Redis:", e)
        return None


mongo_client = connect_mongo()
redis_client = connect_redis()

def redis_key(woj, powiat, params):
    p = ",".join(sorted(params))
    return f"{woj}:{powiat}:{p}"


def get_cached_df(key, loader_func):
    """
    Redis cache ONLY for raw data (list of dicts)
    NEVER cache matplotlib / Qt objects
    """
    if redis_client is not None:
        cached = redis_client.get(key)
        if cached:
            print(f"[REDIS] HIT -> {key}")
            return pd.DataFrame(json.loads(cached))

    print(f"[REDIS] MISS -> {key}")
    df = loader_func()

    if redis_client is not None and not df.empty:
        redis_client.setex(key, 300, json.dumps(df.to_dict("records")))

    return df

def load_from_mongo():
    col = mongo_client["pag_projekt"]["meteo_stats"]
    return pd.DataFrame(list(col.find({}, {"_id": 0})))

class MeteoDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dane meteorologiczne - PAG")
        self.resize(1450, 850)

        self.data = pd.DataFrame()
        self.filtered = pd.DataFrame()

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        filters = QFrame()
        grid = QGridLayout(filters)

        self.woj_box = QComboBox()
        self.pow_box = QComboBox()

        self.chk_temp = QCheckBox("Temperatura [°C]")
        self.chk_wind = QCheckBox("Wiatr [m/s]")
        self.chk_opad = QCheckBox("Opad [mm]")
        self.chk_temp.setChecked(True)

        btn_load = QPushButton("Wczytaj dane")
        btn_show = QPushButton("Pokaż wyniki")

        btn_load.clicked.connect(self.load_data)
        btn_show.clicked.connect(self.apply_filters)

        grid.addWidget(QLabel("Województwo:"), 0, 0)
        grid.addWidget(self.woj_box, 0, 1)
        grid.addWidget(QLabel("Powiat:"), 0, 2)
        grid.addWidget(self.pow_box, 0, 3)

        grid.addWidget(QLabel("Parametry:"), 1, 0)
        grid.addWidget(self.chk_temp, 1, 1)
        grid.addWidget(self.chk_wind, 1, 2)
        grid.addWidget(self.chk_opad, 1, 3)

        grid.addWidget(btn_load, 2, 2)
        grid.addWidget(btn_show, 2, 3)

        self.tabs = QTabWidget()

        self.table = QTableWidget()
        self.tabs.addTab(self.table, "📋 Tabela")

        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)
        tab_plot = QWidget()
        plot_layout = QVBoxLayout(tab_plot)
        plot_layout.addWidget(self.canvas)
        self.tabs.addTab(tab_plot, "📊 Wykresy")

        main_layout.addWidget(filters)
        main_layout.addWidget(self.tabs)

        self._style()
        self.woj_box.currentTextChanged.connect(self.update_powiaty)

    def load_data(self):
        if mongo_client is None:
            QMessageBox.critical(self, "Błąd", "Brak MongoDB")
            return

        print("[INFO] Wczytywanie danych z MongoDB")
        self.data = load_from_mongo()

        self.woj_box.clear()
        self.woj_box.addItems(sorted(self.data["wojewodztwo"].unique()))
        self.update_powiaty()

        QMessageBox.information(self, "OK", "Dane wczytane")

    def update_powiaty(self):
        df = self.data[self.data["wojewodztwo"] == self.woj_box.currentText()]
        self.pow_box.clear()
        self.pow_box.addItem("Wszystkie")
        self.pow_box.addItems(sorted(df["powiat"].unique()))

    def apply_filters(self):
        params = []
        if self.chk_temp.isChecked():
            params.append("temperatura")
        if self.chk_wind.isChecked():
            params.append("wiatr")
        if self.chk_opad.isChecked():
            params.append("opad")

        woj = self.woj_box.currentText()
        powiat = self.pow_box.currentText()

        key = redis_key(woj, powiat, params)

        def loader():
            df = self.data.copy()
            df = df[(df["parametr"].isin(params)) & (df["wojewodztwo"] == woj)]
            if powiat != "Wszystkie":
                df = df[df["powiat"] == powiat]
            return df

        self.filtered = get_cached_df(key, loader)

        self.update_table()
        self.update_plot()
        self.tabs.setCurrentIndex(0)

    def update_table(self):
        df = self.filtered.copy()
        df["jednostka"] = df["parametr"].map(lambda p: PARAM_INFO[p][1])

        cols = ["wojewodztwo", "powiat", "pora_doby", "parametr",
                "srednia", "min", "max", "liczba", "jednostka"]

        self.table.clear()
        self.table.setRowCount(len(df))
        self.table.setColumnCount(len(cols))
        self.table.setHorizontalHeaderLabels(cols)

        for r, (_, row) in enumerate(df.iterrows()):
            for c, col in enumerate(cols):
                val = row[col]
                if isinstance(val, float):
                    val = round(val, 2)
                item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(r, c, item)

        self.table.horizontalHeader().setSectionResizeMode(
            self.table.horizontalHeader().Stretch
        )
        self.table.verticalHeader().setVisible(False)

    def update_plot(self):
        self.figure.clear()
        params = self.filtered["parametr"].unique()

        for i, param in enumerate(params, start=1):
            name, unit, color = PARAM_INFO[param]
            ax = self.figure.add_subplot(1, len(params), i)

            sub = self.filtered[self.filtered["parametr"] == param]
            grp = sub.groupby("pora_doby")["srednia"].mean()

            ax.bar(grp.index, grp.values, color=color, alpha=0.8)
            ax.set_title(name)
            ax.set_ylabel(f"{name} [{unit}]")
            ax.grid(axis="y", alpha=0.3)

        self.figure.tight_layout()
        self.canvas.draw()

    def _style(self):
        self.setStyleSheet("""
        QWidget { font-family: Segoe UI; font-size: 11pt; }
        QFrame { background: #2f3542; color: white; border-radius: 10px; padding: 8px; }
        QLabel { color: white; }
        QPushButton {
            background: #1e90ff;
            color: white;
            padding: 6px;
            border-radius: 6px;
        }
        QPushButton:hover { background: #1c86ee; }
        QHeaderView::section {
            background-color: #1e272e;
            color: white;
            padding: 6px;
            font-weight: bold;
        }
        QTableWidget {
            background-color: #2f3542;
            color: white;
        }
        """)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MeteoDashboard()
    win.show()
    sys.exit(app.exec_())
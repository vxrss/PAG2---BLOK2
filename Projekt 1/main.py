import pandas as pd
import geopandas as gpd
from pymongo import MongoClient

DATA_DIR = r"Meteo_2022-07"
STATIONS_FILE = r"kody_stacji.csv"

WOJ_SHP = r"Dane administracyjne\Dane\woj.shp"
POW_SHP = r"Dane administracyjne\Dane\powiaty.shp"

OUT_CSV = r"\meteo_statystyki_full.csv"

PARAMETRY = {
    "temperatura": "B00300S",
    "wiatr": "B00702A",
    "opad": "B00606S"
}

mongo = MongoClient("mongodb://localhost:27017/")
db = mongo["pag_projekt"]
collection = db["meteo_stats"]

def pora_doby_z_godziny(dt):
    return "dzień" if 6 <= dt.hour < 18 else "noc"

print("▶ Wczytywanie stacji")
stacje = pd.read_csv(STATIONS_FILE, sep=";")
stacje["ID"] = stacje["ID"].astype(str)

def dms_to_float(dms):
    d, m, s = map(float, dms.split())
    return d + m / 60 + s / 3600

stacje["lat"] = stacje["Szerokość geograficzna"].apply(dms_to_float)
stacje["lon"] = stacje["Długość geograficzna"].apply(dms_to_float)

gdf_stacje = gpd.GeoDataFrame(
    stacje,
    geometry=gpd.points_from_xy(stacje["lon"], stacje["lat"]),
    crs="EPSG:4326"
)

print("▶ Wczytywanie województw i powiatów")
woj = gpd.read_file(WOJ_SHP).to_crs("EPSG:4326")
powiaty = gpd.read_file(POW_SHP).to_crs("EPSG:4326")

gdf_stacje = gpd.sjoin(
    gdf_stacje,
    woj[["name", "geometry"]],
    how="left",
    predicate="within"
).rename(columns={"name": "wojewodztwo"}).drop(columns=["index_right"])

gdf_stacje = gpd.sjoin(
    gdf_stacje,
    powiaty[["name", "geometry"]],
    how="left",
    predicate="within"
).rename(columns={"name": "powiat"}).drop(columns=["index_right"])

wyniki = []

for nazwa, kod in PARAMETRY.items():
    print(f"▶ Przetwarzanie: {nazwa}")

    path = f"{DATA_DIR}\\{kod}_2022_07.csv"
    if not pd.io.common.file_exists(path):
        print(f"Brak pliku: {path}")
        continue

    df = pd.read_csv(
        path,
        sep=";",
        header=None,
        usecols=[0, 1, 2, 3],
        names=["kod_stacji", "param", "data", "wartosc"],
        decimal=",",
        dtype={"kod_stacji": str},
        low_memory=False
    )

    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["wartosc"] = pd.to_numeric(df["wartosc"], errors="coerce")
    df = df.dropna()

    if nazwa == "wiatr":
        df = df[df["wartosc"] < 100]

    print(f"   {nazwa}: min={df['wartosc'].min()} max={df['wartosc'].max()}")

    df = df.merge(
        gdf_stacje[["ID", "wojewodztwo", "powiat"]],
        left_on="kod_stacji",
        right_on="ID",
        how="left"
    ).dropna(subset=["wojewodztwo", "powiat"])

    df["pora_doby"] = df["data"].apply(pora_doby_z_godziny)

    grp = df.groupby(
        ["wojewodztwo", "powiat", "pora_doby"]
    )["wartosc"].agg(
        srednia="mean",
        min="min",
        max="max",
        liczba="count"
    ).reset_index()

    grp["parametr"] = nazwa
    wyniki.append(grp)

if not wyniki:
    raise RuntimeError("Brak danych wynikowych")

final_df = pd.concat(wyniki, ignore_index=True)
final_df.to_csv(OUT_CSV, index=False)

collection.delete_many({})
collection.insert_many(final_df.to_dict("records"))

print("Gotowe: CSV + MongoDB (temperatura + wiatr + opad)")

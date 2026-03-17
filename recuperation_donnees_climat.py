"""
Récupération des données climatiques historiques — Open-Meteo Archive API
Défi : https://defis.data.gouv.fr/defis/changement-climatique

v2 — CORRECTIONS :
  - Retry automatique sur erreur 429 (Too Many Requests)
  - Pause progressive entre les tentatives (2s, 10s, 30s, 60s)
  - Reprise depuis les données déjà téléchargées (évite de re-télécharger Bordeaux)
"""

import os
import time
import requests
import pandas as pd

OUTPUT_DIR = "data_climat"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# CONFIGURATION
# =============================================================================

VILLES = {
    "Bordeaux":  {"dept": "33", "lat": 44.8333, "lon": -0.5667, "timezone": "Europe/Paris"},
    "Rennes":    {"dept": "35", "lat": 48.1147, "lon": -1.6794, "timezone": "Europe/Paris"},
    "Nantes":    {"dept": "44", "lat": 47.2184, "lon": -1.5536, "timezone": "Europe/Paris"},
    "Paris":     {"dept": "75", "lat": 48.8566, "lon":  2.3522, "timezone": "Europe/Paris"},
    "Lyon":      {"dept": "69", "lat": 45.72,   "lon":  4.95,   "timezone": "Europe/Paris"},
    "Marseille": {"dept": "13", "lat": 43.44,   "lon":  5.22,   "timezone": "Europe/Paris"},
}

from datetime import date

DATE_DEBUT = "1945-01-01"
DATE_FIN   = date.today().strftime("%Y-%m-%d")

VARIABLES_QUOTIDIENNES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "sunshine_duration",
    "daylight_duration",
    "weathercode",
]

VARIABLES_HORAIRES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
    "cloud_cover",
    "shortwave_radiation",
]

API_BASE = "https://archive-api.open-meteo.com/v1/archive"


# =============================================================================
# FONCTIONS
# =============================================================================

def requete_avec_retry(params: dict, timeout: int = 60,
                       nb_retry: int = 5) -> requests.Response:
    """
    Effectue une requête GET avec retry automatique sur 429.

    Délais progressifs : 2s → 10s → 30s → 60s → 120s
    """
    delais = [2, 10, 30, 60, 120]

    for tentative in range(nb_retry):
        r = requests.get(API_BASE, params=params, timeout=timeout)

        if r.status_code == 200:
            return r

        if r.status_code == 429:
            delai = delais[min(tentative, len(delais) - 1)]
            print(f"\n    429 Too Many Requests — attente {delai}s "
                  f"(tentative {tentative + 1}/{nb_retry})...", end=" ")
            time.sleep(delai)
            continue

        # Autre erreur : lever immédiatement
        r.raise_for_status()

    raise RuntimeError(f"Échec après {nb_retry} tentatives (429 persistant)")


def recuperer_donnees_quotidiennes(ville: str, config: dict,
                                   date_debut: str, date_fin: str) -> pd.DataFrame:
    """Récupère les données quotidiennes pour une ville."""
    params = {
        "latitude":   config["lat"],
        "longitude":  config["lon"],
        "start_date": date_debut,
        "end_date":   date_fin,
        "daily":      ",".join(VARIABLES_QUOTIDIENNES),
        "timezone":   config["timezone"],
        "wind_speed_unit":    "kmh",
        "precipitation_unit": "mm",
    }

    r = requete_avec_retry(params)
    data = r.json()

    df = pd.DataFrame(data["daily"])
    df["time"] = pd.to_datetime(df["time"])
    df = df.rename(columns={
        "time":                        "DATE",
        "temperature_2m_max":          "TX",
        "temperature_2m_min":          "TN",
        "temperature_2m_mean":         "TM",
        "precipitation_sum":           "RR",
        "rain_sum":                    "RR_pluie",
        "snowfall_sum":                "neige_cm",
        "precipitation_hours":         "heures_precip",
        "wind_speed_10m_max":          "vent_max_kmh",
        "wind_gusts_10m_max":          "rafales_max_kmh",
        "wind_direction_10m_dominant": "vent_direction",
        "shortwave_radiation_sum":     "rayonnement_MJ",
        "et0_fao_evapotranspiration":  "evapotranspiration",
        "sunshine_duration":           "ensoleillement_s",
        "daylight_duration":           "duree_jour_s",
        "weathercode":                 "code_meteo",
    })

    df["ville"]       = ville
    df["departement"] = config["dept"]
    df["latitude"]    = config["lat"]
    df["longitude"]   = config["lon"]

    if "ensoleillement_s" in df.columns:
        df["ensoleillement_h"] = (df["ensoleillement_s"] / 3600).round(2)
        df["duree_jour_h"]     = (df["duree_jour_s"]     / 3600).round(2)
        df = df.drop(columns=["ensoleillement_s", "duree_jour_s"])

    return df


def recuperer_donnees_horaires(ville: str, config: dict,
                                date_debut: str, date_fin: str) -> pd.DataFrame:
    """Récupère les données horaires pour une ville (~210 000 lignes/ville)."""
    params = {
        "latitude":   config["lat"],
        "longitude":  config["lon"],
        "start_date": date_debut,
        "end_date":   date_fin,
        "hourly":     ",".join(VARIABLES_HORAIRES),
        "timezone":   config["timezone"],
        "wind_speed_unit":    "kmh",
        "precipitation_unit": "mm",
    }

    r = requete_avec_retry(params, timeout=120)
    data = r.json()

    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    df = df.rename(columns={
        "time":                 "DATETIME",
        "temperature_2m":       "T",
        "relative_humidity_2m": "HR",
        "precipitation":        "RR",
        "wind_speed_10m":       "vent_kmh",
        "wind_direction_10m":   "vent_direction",
        "surface_pressure":     "pression_hPa",
        "cloud_cover":          "nuages_pct",
        "shortwave_radiation":  "rayonnement_Wm2",
    })

    df["ville"]       = ville
    df["departement"] = config["dept"]

    return df


def recuperer_toutes_villes(villes: dict = VILLES,
                             date_debut: str = DATE_DEBUT,
                             date_fin: str = DATE_FIN,
                             horaire: bool = False,
                             villes_deja_telechargees: list = None,
                             pause: float = 3.0) -> tuple:
    """
    Récupère les données pour toutes les villes.

    Parameters
    ----------
    villes_deja_telechargees : liste de noms de villes à ignorer
                               (déjà présentes dans le fichier de sortie)
    pause                    : secondes entre chaque ville (évite les 429)
    """
    if villes_deja_telechargees is None:
        villes_deja_telechargees = []

    frames_q = []
    frames_h = []

    for ville, config in villes.items():
        if ville in villes_deja_telechargees:
            print(f"  [{ville}] Déjà téléchargé — ignoré")
            continue

        # ── Quotidien ─────────────────────────────────────────────────────────
        print(f"  [{ville}] Quotidien {date_debut} → {date_fin}...", end=" ", flush=True)
        try:
            df_q = recuperer_donnees_quotidiennes(ville, config, date_debut, date_fin)
            frames_q.append(df_q)
            print(f"✓ {len(df_q):,} jours")
        except Exception as e:
            print(f"\n  ⚠ Erreur {ville} : {e}")

        # ── Horaire (optionnel) ────────────────────────────────────────────────
        if horaire:
            print(f"  [{ville}] Horaire {date_debut} → {date_fin}...", end=" ", flush=True)
            try:
                df_h = recuperer_donnees_horaires(ville, config, date_debut, date_fin)
                frames_h.append(df_h)
                print(f"✓ {len(df_h):,} heures")
            except Exception as e:
                print(f"\n  ⚠ Erreur horaire {ville} : {e}")

        time.sleep(pause)

    df_q_all = pd.concat(frames_q, ignore_index=True) if frames_q else pd.DataFrame()
    df_h_all = pd.concat(frames_h, ignore_index=True) if frames_h else pd.DataFrame()

    return df_q_all, df_h_all


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    print("=" * 60)
    print("RÉCUPÉRATION DONNÉES CLIMATIQUES — Open-Meteo Archive API")
    print("Villes : " + " | ".join(f"{v} ({c['dept']})" for v, c in VILLES.items()))
    print(f"Période cible : {DATE_DEBUT} → {DATE_FIN}")
    print("=" * 60)

    chemin_q = os.path.join(OUTPUT_DIR, "observations_quotidiennes.csv")

    # 1. Chargement de l'existant pour incrémentalité
    df_existant = pd.DataFrame()
    if os.path.exists(chemin_q):
        df_existant = pd.read_csv(chemin_q, sep=";")
        df_existant["DATE"] = pd.to_datetime(df_existant["DATE"])

    nouvelles_frames = []

    for ville, config in VILLES.items():
        debut_ville = DATE_DEBUT
        
        # Vérifier si on a déjà des données pour cette ville
        if not df_existant.empty and "ville" in df_existant.columns:
            v_data = df_existant[df_existant["ville"] == ville]
            if not v_data.empty:
                derniere_date = v_data["DATE"].max()
                # On repart du lendemain de la dernière date
                debut_ville = (derniere_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        
        if debut_ville > DATE_FIN:
            print(f"  [{ville}] Déjà à jour ({DATE_FIN})")
            continue

        print(f"  [{ville}] Récupération : {debut_ville} → {DATE_FIN}...", end=" ", flush=True)
        try:
            df_q = recuperer_donnees_quotidiennes(ville, config, debut_ville, DATE_FIN)
            if not df_q.empty:
                nouvelles_frames.append(df_q)
                print(f"✓ {len(df_q):,} nouveaux jours")
            else:
                print("— Déjà à jour.")
        except Exception as e:
            print(f"\n  ⚠ Erreur {ville} : {e}")
        
        time.sleep(2) # Pause respectueuse

    # 2. Fusion et Sauvegarde
    if nouvelles_frames:
        df_final = pd.concat([df_existant] + nouvelles_frames, ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["ville", "DATE"], keep="last")
        df_final = df_final.sort_values(["ville", "DATE"]).reset_index(drop=True)
        
        df_final.to_csv(chemin_q, index=False, sep=";")
        print(f"\n✓ Mise à jour terminée → {chemin_q} ({len(df_final)} lignes)")
    else:
        print("\n— Aucune nouvelle donnée à ajouter.")

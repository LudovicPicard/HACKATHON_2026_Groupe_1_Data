"""
Récupération des données "Changement Climatique" - Météo-France DPClim v1
Défi : https://defis.data.gouv.fr/defis/changement-climatique

v6 — CORRECTION PRINCIPALE :
  - La commande répond HTTP 202 (Accepted) et non 200
  - raise_for_status() lève une exception sur 202 car ce n'est pas 200
  → On accepte maintenant 202 et on lit le corps pour récupérer l'id_commande
"""

import os
import io
import time
import zipfile
import requests
import pandas as pd

# ─── Token ───────────────────────────────────────────────────────────────────
METEO_API_KEY = os.environ.get(
    "METEO_API_KEY",
    "eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJMdWRvdmljMTBAY2FyYm9uLnN1cGVyIiwiYXBwbGljYXRpb24iOnsib3duZXIiOiJMdWRvdmljMTAiLCJ0aWVyUXVvdGFUeXBlIjpudWxsLCJ0aWVyIjoiVW5saW1pdGVkIiwibmFtZSI6IkRlZmF1bHRBcHBsaWNhdGlvbiIsImlkIjozODQ2NSwidXVpZCI6IjRlYjJjMWM0LTIxNzctNDQzMi05ZDlmLTU3NTA1ZDg5OTRlMyJ9LCJpc3MiOiJodHRwczpcL1wvcG9ydGFpbC1hcGkubWV0ZW9mcmFuY2UuZnI6NDQzXC9vYXV0aDJcL3Rva2VuIiwidGllckluZm8iOnsiNTBQZXJNaW4iOnsidGllclF1b3RhVHlwZSI6InJlcXVlc3RDb3VudCIsImdyYXBoUUxNYXhDb21wbGV4aXR5IjowLCJncmFwaFFMTWF4RGVwdGgiOjAsInN0b3BPblF1b3RhUmVhY2giOnRydWUsInNwaWtlQXJyZXN0TGltaXQiOjAsInNwaWtlQXJyZXN0VW5pdCI6InNlYyJ9fSwia2V5dHlwZSI6IlBST0RVQ1RJT04iLCJzdWJzY3JpYmVkQVBJcyI6W3sic3Vic2NyaWJlclRlbmFudERvbWFpbiI6ImNhcmJvbi5zdXBlciIsIm5hbWUiOiJEb25uZWVzUHVibGlxdWVzQ2xpbWF0b2xvZ2llIiwiY29udGV4dCI6IlwvcHVibGljXC9EUENsaW1cL3YxIiwicHVibGlzaGVyIjoiYWRtaW5fbWYiLCJ2ZXJzaW9uIjoidjEiLCJzdWJzY3JpcHRpb25UaWVyIjoiNTBQZXJNaW4ifV0sImV4cCI6MTc3Mzk5OTkwOCwidG9rZW5fdHlwZSI6ImFwaUtleSIsImlhdCI6MTc3MzY1NDMwOCwianRpIjoiZGYzMzliNjYtN2I0OS00ZmQ4LTg4ZjMtYjFkOTI4NGNhOTBlIn0=.HTbEwpcunVqAl_y0mfzRypVbdWk7cK9El27rsdilcGH76esd5YRNGbSuPRVSmlRd_RTKgILb2mqhBh9u5JEAKIyet9XteWEsjIu6y97RPa_eccR-qksyGMakygtChywnTvexoFTtC2RHMU0a9tk31EKRtDULEB3DhlZ2_tGERNJHezkQwRKmZ98Avpj6qU4hGKJF_HICMD6zkv4A_qnHhnVEROq3EVf3s3VnBAf_RLeaDpGEKbSOIqJmjLf9lcNxquSzUBGkOeXg1MuwgbSGgEEbHBZrCz05q6Mpu4aMGeszj3l4AFkeDs4KkTVmXJ1YTVDFI1yILLFfxVypmr5ohg=="
)

METEO_API_BASE = "https://public-api.meteofrance.fr/public/DPClim/v1"
OUTPUT_DIR = "data_climat"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DEPARTEMENTS = {
    "33": "Gironde (Bordeaux)",
    "35": "Ille-et-Vilaine (Rennes)",
    "44": "Loire-Atlantique (Nantes)",
    "75": "Paris",
}


# =============================================================================
# AUTHENTIFICATION
# =============================================================================

def detecter_headers() -> dict:
    url = f"{METEO_API_BASE}/liste-stations/quotidienne"
    params = {"id-departement": "33"}
    candidats = [
        ("apikey",               {"apikey": METEO_API_KEY}),
        ("Authorization Bearer", {"Authorization": f"Bearer {METEO_API_KEY}"}),
        ("Authorization apikey", {"Authorization": f"apikey {METEO_API_KEY}"}),
    ]
    print("Détection du format d'authentification...")
    for nom, headers in candidats:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=15)
            print(f"  [{nom:30s}] → HTTP {r.status_code}")
            if r.status_code in (200, 204):
                print(f"  ✓ Format valide : {nom}\n")
                return headers
        except requests.RequestException as e:
            print(f"  [{nom:30s}] → Erreur : {e}")
    raise RuntimeError("Auth impossible. Régénérez votre token sur https://portail-api.meteofrance.fr/")


HEADERS = detecter_headers()


# =============================================================================
# FONCTIONS API
# =============================================================================

def api_lister_stations_departement(id_departement: str) -> pd.DataFrame:
    url = f"{METEO_API_BASE}/liste-stations/quotidienne"
    r = requests.get(url, headers=HEADERS, params={"id-departement": id_departement}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["departement"] = id_departement
    return df


def filtrer_stations_actives(df: pd.DataFrame) -> pd.DataFrame:
    avant = len(df)
    if "posteOuvert" in df.columns:
        df = df[df["posteOuvert"].astype(str).str.lower() == "true"]
    if "postePublic" in df.columns:
        df = df[df["postePublic"].astype(str).str.lower() == "true"]
    print(f"  Filtrage : {avant} → {len(df)} stations (ouvertes et publiques)")
    return df.reset_index(drop=True)


def api_commande(id_station: str, date_debut: str, date_fin: str,
                 frequence: str = "quotidienne") -> str:
    """
    Soumet une commande de données.

    ✅ L'API répond HTTP 202 (Accepted) — pas 200.
       raise_for_status() traite 202 comme une erreur, donc on vérifie
       manuellement le status_code.
    """
    url = f"{METEO_API_BASE}/commande-station/{frequence}"
    params = {
        "id-station": id_station,
        "date-deb-periode": date_debut + "T00:00:00Z",
        "date-fin-periode": date_fin   + "T00:00:00Z",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)

    # ✅ 202 = commande acceptée (normal), 200 aussi accepté
    if r.status_code not in (200, 202):
        raise requests.HTTPError(
            f"HTTP {r.status_code} — {r.text[:300]}",
            response=r
        )

    data = r.json()
    id_cmd = (
        data.get("elaboreProduitAvecDemandeResponse", {}).get("return")
        or data.get("id")
        or data.get("idCmde")
    )
    if not id_cmd:
        raise ValueError(f"Impossible d'extraire l'id_commande : {data}")

    print(f"    Commande acceptée : {id_cmd}")
    return str(id_cmd)


def api_recuperer_commande(id_commande: str, attente_max: int = 300) -> pd.DataFrame:
    """
    Attend que la commande soit prête et récupère le fichier.

    Codes de réponse attendus sur /commande/fichier :
      201 → en cours de traitement, on réessaie
      200 → fichier prêt, on télécharge
    """
    url = f"{METEO_API_BASE}/commande/fichier"
    params = {"id-cmde": id_commande}
    delai = 5

    for tentative in range(attente_max // delai):
        r = requests.get(url, headers=HEADERS, params=params, timeout=60)

        if r.status_code == 201:
            print(f"    En attente du fichier... ({tentative * delai}s)", end="\r")
            time.sleep(delai)
            continue

        if r.status_code != 200:
            raise requests.HTTPError(
                f"HTTP {r.status_code} sur /commande/fichier — {r.text[:200]}",
                response=r
            )

        print(f"    ✓ Fichier reçu ({len(r.content) / 1024:.1f} Ko)          ")

        # ZIP ou CSV direct
        if r.content[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                frames = [
                    pd.read_csv(io.StringIO(z.read(n).decode("utf-8")), sep=";")
                    for n in z.namelist() if n.endswith(".csv")
                ]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        else:
            return pd.read_csv(io.StringIO(r.text), sep=";")

    print(f"\n    ⚠ Timeout après {attente_max}s — commande {id_commande}")
    return pd.DataFrame()


def api_donnees_station(id_station: str, date_debut: str, date_fin: str,
                        frequence: str = "quotidienne") -> pd.DataFrame:
    """Récupère les données d'une station (commande 202 + attente + fichier)."""
    id_cmd = api_commande(id_station, date_debut, date_fin, frequence)
    return api_recuperer_commande(id_cmd)


def recuperer_stations_cibles(departements: dict = DEPARTEMENTS,
                               filtre_actives: bool = True) -> pd.DataFrame:
    frames = []
    for dept, ville in departements.items():
        print(f"\n  Département {dept} — {ville}")
        df = api_lister_stations_departement(dept)
        if not df.empty:
            if filtre_actives:
                df = filtrer_stations_actives(df)
            df["ville"] = ville
            frames.append(df)
        time.sleep(0.5)
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    print(f"\n  ✓ {len(df_all)} stations retenues au total")
    return df_all


def recuperer_donnees_departements(
    df_stations: pd.DataFrame,
    date_debut: str,
    date_fin: str,
    frequence: str = "quotidienne",
    pause: float = 1.5,
    chemin_sortie: str = None,
) -> pd.DataFrame:
    frames = []
    ignorees = []
    col_id  = "id"  if "id"  in df_stations.columns else df_stations.columns[0]
    col_nom = "nom" if "nom" in df_stations.columns else df_stations.columns[1]
    total = len(df_stations)

    for i, row in df_stations.iterrows():
        id_station  = str(row[col_id])
        nom_station = str(row.get(col_nom, id_station))
        dept        = str(row.get("departement", "?"))
        print(f"\n  [{len(frames)+1}/{total}] {nom_station} (dept {dept}) — id {id_station}")

        try:
            df = api_donnees_station(id_station, date_debut, date_fin, frequence)
            if not df.empty:
                df["id_station"]  = id_station
                df["nom_station"] = nom_station
                df["departement"] = dept
                frames.append(df)
                if chemin_sortie:
                    pd.concat(frames, ignore_index=True).to_csv(
                        chemin_sortie, index=False, sep=";"
                    )
            else:
                print(f"    ⚠ Fichier vide")
                ignorees.append((id_station, nom_station, "fichier vide"))

        except requests.HTTPError as e:
            print(f"    ⚠ {str(e)[:150]}")
            ignorees.append((id_station, nom_station, str(e)[:100]))
        except Exception as e:
            print(f"    ⚠ {e}")
            ignorees.append((id_station, nom_station, str(e)))

        time.sleep(pause)

    print(f"\n{'='*60}")
    print(f"  Stations avec données : {len(frames)}/{total}")
    print(f"  Stations ignorées     : {len(ignorees)}/{total}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    print(f"✓ {len(combined):,} observations | {len(frames)} stations")
    return combined


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    # ── ÉTAPE 1 : Stations ────────────────────────────────────────────────────
    print("=" * 60)
    print("ÉTAPE 1 — STATIONS DES DÉPARTEMENTS CIBLES")
    print("  " + " | ".join(f"{k} ({v})" for k, v in DEPARTEMENTS.items()))
    print("=" * 60)

    df_stations = recuperer_stations_cibles(filtre_actives=True)
    cols_affichage = [c for c in ["id", "nom", "lat", "lon", "alt", "departement"] if c in df_stations.columns]
    print(df_stations[cols_affichage].to_string(index=False))

    chemin_stations = os.path.join(OUTPUT_DIR, "stations_cibles.csv")
    df_stations.to_csv(chemin_stations, index=False, sep=";")
    print(f"\n→ {len(df_stations)} stations sauvegardées dans {chemin_stations}")

    # ── ÉTAPE 2 : Données quotidiennes (2000–2023) ────────────────────────────
    print("\n" + "=" * 60)
    print("ÉTAPE 2 — DONNÉES QUOTIDIENNES (2000–2023)")
    print("=" * 60)

    chemin_obs = os.path.join(OUTPUT_DIR, "observations_quotidiennes.csv")

    df_obs = recuperer_donnees_departements(
        df_stations=df_stations,
        date_debut="2000-01-01",
        date_fin="2023-12-31",
        frequence="quotidienne",
        chemin_sortie=chemin_obs,
    )
    if not df_obs.empty:
        print(f"\nColonnes : {list(df_obs.columns)}")
        print(df_obs.head())

    # ── ÉTAPE 3 : Données mensuelles (1950–2023) ──────────────────────────────
    print("\n" + "=" * 60)
    print("ÉTAPE 3 — DONNÉES MENSUELLES (1950–2023)")
    print("=" * 60)

    chemin_mens = os.path.join(OUTPUT_DIR, "observations_mensuelles.csv")

    df_mens = recuperer_donnees_departements(
        df_stations=df_stations,
        date_debut="1950-01-01",
        date_fin="2023-12-31",
        frequence="mensuelle",
        chemin_sortie=chemin_mens,
    )
    if not df_mens.empty:
        print(f"\nColonnes : {list(df_mens.columns)}")
        print(df_mens.head())

    print(f"\n✓ Terminé. Fichiers dans : {os.path.abspath(OUTPUT_DIR)}")

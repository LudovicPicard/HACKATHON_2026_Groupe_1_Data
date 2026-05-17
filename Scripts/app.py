import streamlit as st
import pandas as pd
import plotly.express as px
import os
import numpy as np

# 1. Configuration de la page
st.set_page_config(page_title="Climate Watch France - National", layout="wide", page_icon="🌍")

# 2. Configuration des chemins
PATH_DATA = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Datas"
FILE_DEPT = os.path.join(PATH_DATA, "gold_indicators_departement.parquet")
FILE_REG = os.path.join(PATH_DATA, "gold_indicators_region.parquet")

# 3. Chargement optimisé des données (Couche Gold uniquement pour la vitesse)
@st.cache_data
def load_gold_data():
    if not os.path.exists(FILE_DEPT) or not os.path.exists(FILE_REG):
        st.error("❌ Fichiers Gold introuvables. Veuillez exécuter 'silver_to_gold.py' d'abord.")
        return None, None
    
    df_dept = pd.read_parquet(FILE_DEPT)
    df_reg = pd.read_parquet(FILE_REG)
    
    # Sécurité temporelle : filtrage des données futures éventuelles
    df_dept = df_dept[df_dept['ANNEE'] <= 2025]
    df_reg = df_reg[df_reg['ANNEE'] <= 2025]
    
    return df_dept, df_reg

df_dept, df_reg = load_gold_data()

# --- TITRE DE L'APPLICATION ---
st.title("🌍 Climate Watch : Observatoire National")
st.markdown("Analyses climatiques à l'échelle nationale basées sur les données ouvertes de Météo-France.")

# Vérification du chargement
if df_dept is not None and df_reg is not None:
    
    tab1, tab2 = st.tabs(["📊 Zoom Territorial (Régions & Départements)", "🗺️ Comparatif National"])

    # ---------------------------------------------------------
    # TAB 1 : ZOOM TERRITORIAL (Filtres dynamiques en cascade)
    # ---------------------------------------------------------
    with tab1:
        st.subheader("🔍 Analyse fine par échelon territorial")
        
        # Filtres côte à côte
        f_col1, f_col2, f_col3 = st.columns([2, 2, 3])
        
        with f_col1:
            liste_regions = sorted(df_dept['REGION'].unique())
            region_sel = st.selectbox("1. Choisir une Région", liste_regions)
            
        with f_col2:
            # Filtrage dynamique de la liste des départements selon la région sélectionnée
            depts_disponibles = sorted(df_dept[df_dept['REGION'] == region_sel]['DEPARTEMENT'].unique())
            dept_sel = st.selectbox("2. Choisir un Département", depts_disponibles)
            
        with f_col3:
            year_range = st.slider("Période d'analyse", 1950, 2025, (1990, 2025), key="s1")

        # Filtrage final des DataFrames
        df_dept_filtered = df_dept[
            (df_dept['DEPARTEMENT'] == dept_sel) & 
            (df_dept['ANNEE'].between(year_range[0], year_range[1]))
        ]
        
        # Métriques clés sur la période sélectionnée
        st.markdown(f"### 📍 Indicateurs pour le département **{dept_sel}** ({region_sel})")
        m1, m2, m3, m4 = st.columns(4)
        
        m1.metric("Anomalie Thermique Moyenne", f"{df_dept_filtered['ANOMALIE_TM'].mean():+.2f} °C")
        m2.metric("Moyenne des Jours >30°C", f"{df_dept_filtered['IS_HEATWAVE'].mean():.1f} j/an")
        m3.metric("Pluviométrie Moyenne", f"{df_dept_filtered['RR'].mean():.0f} mm/an")
        m4.metric("Moyenne des Jours de Gel", f"{df_dept_filtered['IS_FROST'].mean():.1f} j/an")

        # Graphiques d'évolution historique
        g_col1, g_col2 = st.columns(2)
        
        with g_col1:
            fig_temp = px.bar(
                df_dept_filtered, x='ANNEE', y='ANOMALIE_TM',
                title=f"Échauffement climatique : Évolution des anomalies thermiques ({dept_sel})",
                color='ANOMALIE_TM', color_continuous_scale='RdBu_r', color_continuous_midpoint=0
            )
            st.plotly_chart(fig_temp, use_container_width=True)
            
        with g_col2:
            fig_rain = px.line(
                df_dept_filtered, x='ANNEE', y='RR',
                title=f"Évolution des précipitations annuelles ({dept_sel})",
                markers=True
            )
            fig_rain.update_traces(line_color='#2ca02c')
            st.plotly_chart(fig_rain, use_container_width=True)

        # Graphique des extrêmes climatiques
        st.markdown("---")
        st.subheader("🔥 Focus sur les extrêmes : Canicules vs Gel nocturne")
        
        fig_extremes = px.area(
            df_dept_filtered, x='ANNEE', y=['IS_HEATWAVE', 'IS_FROST'],
            labels={'value': 'Nombre de jours par an', 'variable': 'Indicateurs'},
            title=f"Tendances croisées des extrêmes thermiques ({dept_sel})",
            color_discrete_map={'IS_HEATWAVE': '#ef553b', 'IS_FROST': '#636efa'}
        )
        st.plotly_chart(fig_extremes, use_container_width=True)

    # ---------------------------------------------------------
    # TAB 2 : COMPARATIF NATIONAL (Régional macro)
    # ---------------------------------------------------------
    with tab2:
        st.subheader("⚖️ Tableau de bord comparatif des Régions de France")
        
        # Fenêtre temporelle pour la photo globale
        latest_year = int(df_reg['ANNEE'].max())
        st.info(f"Les classements et comparaisons ci-dessous sont calculés sur la base des données de la dernière année consolidée : **{latest_year}**.")
        
        df_reg_latest = df_reg[df_reg['ANNEE'] == latest_year]

        # Sélection de l'indicateur par l'utilisateur
        indicator = st.radio(
            "Indicateur à mettre en valeur :", 
            ["Anomalie Température (°C)", "Jours de Canicule (>30°C)", "Cumul des Précipitations (mm)", "Nuits Tropicales (>=20°C)"], 
            horizontal=True
        )
        
        # Mapping des variables
        target_col = 'ANOMALIE_TM' if "Température" in indicator else (
            'IS_HEATWAVE' if "Canicule" in indicator else (
            'RR' if "Précipitations" in indicator else 'IS_TROPICAL_NIGHT'
            )
        )
        
        # Adaptation des couleurs selon l'indicateur
        color_scale = 'Blues' if target_col == 'RR' else 'YlOrRd'
        ascending_sort = True if target_col == 'IS_FROST' else False

        c1, c2 = st.columns([3, 2])

        with c1:
            # Graphique à barres comparatif de toutes les régions
            fig_bar = px.bar(
                df_reg_latest.sort_values(by=target_col, ascending=ascending_sort),
                x=target_col, y='REGION', orientation='h',
                color=target_col, color_continuous_scale=color_scale,
                title=f"Classement national des Régions face à l'indicateur : {indicator} ({latest_year})",
                text_auto='.1f'
            )
            fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, height=550)
            st.plotly_chart(fig_bar, use_container_width=True)

        with c2:
            st.markdown(f"🏆 **Données brutes par Région ({latest_year})**")
            df_rank = df_reg_latest[['REGION', target_col]].sort_values(by=target_col, ascending=ascending_sort)
            
            # Renommer la colonne pour un rendu propre à l'écran
            df_rank.columns = ['Région', indicator]
            st.dataframe(df_rank, use_container_width=True, hide_index=True, height=500)

        # Matrice d'analyse globale de corrélation
        st.markdown("---")
        st.subheader("📈 Profiling National : Corrélation Climat / Risques")
        
        fig_corr = px.scatter(
            df_reg_latest, x='RR', y='IS_HEATWAVE', text='REGION', 
            size=df_reg_latest['ANOMALIE_TM'].clip(lower=0.1), color='ANOMALIE_TM',
            labels={'RR': 'Précipitations Annuelles Moyennes (mm)', 'IS_HEATWAVE': 'Nombre de jours de canicule (>30°C)'},
            title="Cartographie des régions : Volume de pluie vs Intensité des fortes chaleurs",
            color_continuous_scale='Reds', height=500
            )
        st.plotly_chart(fig_corr, use_container_width=True)
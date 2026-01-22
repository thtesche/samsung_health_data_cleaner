import streamlit as st
import pandas as pd
import plotly.express as px
import io
import datetime
import numpy as np

# Grundeinstellungen der Seite
st.set_page_config(page_title="Samsung Health Analyzer", layout="wide")

st.title("📊 Samsung Health Professional Dashboard")
st.markdown("""
Lade deine Samsung Health CSV-Dateien hoch (Sauerstoff, Puls, etc.). 
Dieses Tool bereinigt die Daten automatisch und ermöglicht eine detaillierte Nacht-Analyse.
""")

# --- DATEI UPLOAD ---
uploaded_files = st.file_uploader(
    "Samsung CSV-Dateien auswählen", 
    type=['csv'], 
    accept_multiple_files=True
)

if uploaded_files:
    all_dfs = []
    
    for uploaded_file in uploaded_files:
        # Datei einlesen
        raw_text = uploaded_file.read().decode("utf-8", errors="ignore")
        
        # Wir überspringen die allererste Zeile (Samsung Metadaten)
        # index_col=False verhindert das Verrutschen der Spalten
        df_temp = pd.read_csv(
            io.StringIO(raw_text), 
            skiprows=1, 
            index_col=False, 
            engine='python'
        )
        
        # Spaltennamen säubern (entfernt die langen Präfixe)
        df_temp.columns = [c.split('.')[-1].strip() for c in df_temp.columns]
        
        # Dateiname zur Unterscheidung hinzufügen
        df_temp['Source'] = uploaded_file.name
        all_dfs.append(df_temp)

    # Alle Dateien zu einem großen Datensatz verbinden
    df = pd.concat(all_dfs, axis=0, ignore_index=True)

    # --- DATUM KONVERTIERUNG ---
    if 'start_time' in df.columns:
        # Konvertierung mit dem Millisekunden-Format
        df['start_time'] = pd.to_datetime(df['start_time'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        
        # Ungültige Zeilen entfernen
        df = df.dropna(subset=['start_time'])
        df = df.sort_values('start_time')

        # --- SIDEBAR / FILTER ---
        st.sidebar.header("🛠️ Filter & Einstellungen")
        
        # 1. Datums-Filter
        min_date = df['start_time'].min().to_pydatetime()
        max_date = df['start_time'].max().to_pydatetime()
        
        selected_dates = st.sidebar.slider(
            "Zeitraum wählen", 
            min_date, max_date, 
            (min_date, max_date),
            format="DD.MM.YY"
        )
        
        # Filter anwenden
        mask = (df['start_time'] >= selected_dates[0]) & (df['start_time'] <= selected_dates[1])
        filtered_df = df.loc[mask].copy()

        # 2. Nacht-Modus Filter
        st.sidebar.markdown("---")
        st.sidebar.subheader("🌙 Nacht-Modus")
        use_night_filter = st.sidebar.checkbox("Zeitfenster einschränken", value=False)
        
        if use_night_filter:
            start_t = st.sidebar.time_input("Von", datetime.time(21, 0))
            end_t = st.sidebar.time_input("Bis", datetime.time(4, 30))
            
            def is_in_range(t):
                if start_t <= end_t:
                    return start_t <= t <= end_t
                else: # Über Mitternacht (z.B. 21:00 - 04:30)
                    return t >= start_t or t <= end_t
            
            filtered_df['temp_time'] = filtered_df['start_time'].dt.time
            filtered_df = filtered_df[filtered_df['temp_time'].apply(is_in_range)]

        # --- VISUALISIERUNG ---
        if not filtered_df.empty:
            # Automatische Auswahl numerischer Spalten (Messwerte)
            num_cols = filtered_df.select_dtypes(include=['number']).columns.tolist()
            # IDs und unbrauchbare Spalten entfernen
            blacklist = ['tag_id', 'source', 'coverage_rate', 'client_data_ver', 'custom']
            clean_cols = [c for c in num_cols if c not in blacklist]
            
            st.subheader("Grafische Auswertung")
            y_axis = st.multiselect("Welche Werte anzeigen?", clean_cols, default=clean_cols[:1])
            
            if y_axis:
                fig = px.scatter(
                    filtered_df, 
                    x='start_time', 
                    y=y_axis,
                    color='Source' if len(uploaded_files) > 1 else None,
                    template="plotly_dark",
                    opacity=0.5 # Punkte etwas blasser, damit die Linie besser auffällt
                )
            
                # Verhindert hässliche Linien durch Datenlücken (z.B. tagsüber)
                fig.update_traces(connectgaps=False)
                
                # --- POLYNOMISCHE INTERPOLATION ---
                st.sidebar.markdown("---")
                st.sidebar.subheader("📈 Trend-Linie")
                show_trend = st.sidebar.checkbox("Interpolation anzeigen", value=False)
                degree = st.sidebar.slider("Grad des Polynoms", 1, 10, 3)

                if show_trend and len(filtered_df) > degree:
                    for col in y_axis:
                        # Zeit in Zahlen umwandeln für die Berechnung
                        x_numeric = pd.to_numeric(filtered_df['start_time'])
                        y_values = filtered_df[col]
                        
                        # Polynom berechnen (Fit)
                        weights = np.polyfit(x_numeric, y_values, degree)
                        model = np.poly1d(weights)
                        
                        # Trend-Werte berechnen
                        trend_line = model(x_numeric)
                        
                        # Trend-Linie zum Diagramm hinzufügen
                        fig.add_scatter(
                            x=filtered_df['start_time'], 
                            y=trend_line, 
                            mode='lines', 
                            name=f'Trend {col} (Grad {degree})',
                            line=dict(width=3)
                        )

                st.plotly_chart(fig, width="stretch")
            
            # --- STATISTIKEN ---
            st.markdown("---")
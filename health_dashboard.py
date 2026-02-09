import streamlit as st
import pandas as pd
import plotly.express as px
import io
import datetime
import numpy as np

# Page basic settings
st.set_page_config(page_title="Samsung Health Analyzer", layout="wide")

st.title("📊 Samsung Health Professional Dashboard")
st.markdown("""
Upload your Samsung Health CSV files (Oxygen, Pulse, etc.). 
This tool automatically cleans the data and enables a detailed night analysis.
""")

# --- FILE UPLOAD ---
uploaded_files = st.file_uploader(
    "Select Samsung CSV files",
    type=['csv'],
    accept_multiple_files=True
)

if uploaded_files:
    all_dfs = []

    for uploaded_file in uploaded_files:
        # Read file
        raw_text = uploaded_file.read().decode("utf-8", errors="ignore")

        # We skip the very first line (Samsung metadata)
        # index_col=False prevents column shifting
        df_temp = pd.read_csv(
            io.StringIO(raw_text),
            index_col=False,
            engine='python'
        )

        # Clean column names (removes long prefixes)
        df_temp.columns = [c.split('.')[-1].strip() for c in df_temp.columns]

        # Add filename for distinction
        df_temp['Source'] = uploaded_file.name
        all_dfs.append(df_temp)

    # Combine all files into one large dataset
    df: pd.DataFrame = pd.concat(all_dfs, axis=0, ignore_index=True)

    # --- DATA TRANSFORMATION: MAPPINGS ---

    # Sleep Stage Mapping
    if 'stage' in df.columns and pd.api.types.is_numeric_dtype(df['stage']):
        sleep_stage_mapping = {
            40001: 'Awake',
            40002: 'Light sleep',
            40003: 'Deep sleep',
            40004: 'REM sleep'
        }
        df['stage'] = pd.to_numeric(df['stage'], errors='coerce').map(sleep_stage_mapping)
        df.dropna(subset=['stage'], inplace=True)

    # ECG Symptom Mapping
    if 'symptoms' in df.columns:
        # Check if the column contains string representations of lists like '[]' or '[1]'
        if df['symptoms'].dtype == 'object' and df['symptoms'].astype(str).str.contains(r'\[', na=False).any():
            symptom_mapping = {
                0: 'None',
                1: 'Shortness of breath',
                2: 'Fatigue',
                3: 'Dizziness',
                4: 'Chest pain/pressure',
                5: 'Palpitations',
                6: 'Faintness'
            }

            def extract_symptom_id(symptom_val):
                if pd.isna(symptom_val): return 0
                symptom_str = str(symptom_val).strip()
                if not symptom_str or symptom_str == '[]': return 0
                try:
                    num_str = symptom_str.replace('[', '').replace(']', '').split(',')[0].strip()
                    return int(num_str) if num_str else 0
                except (ValueError, IndexError):
                    return 0

            df['symptoms'] = df['symptoms'].apply(extract_symptom_id).map(symptom_mapping)

    # ECG Classification Mapping
    if 'classification' in df.columns and pd.api.types.is_numeric_dtype(df['classification']):
        classification_mapping = {
            1: 'Sinus rhythm',
            2: 'Atrial fibrillation',
            3: 'Inconclusive',
            4: 'Poor recording'
        }
        df['classification'] = pd.to_numeric(df['classification'], errors='coerce').map(classification_mapping)
        df.dropna(subset=['classification'], inplace=True)

    # Food Intake Meal Type Mapping
    if 'meal_type' in df.columns and pd.api.types.is_numeric_dtype(df['meal_type']):
        meal_type_mapping = {
            100001: 'Breakfast',
            100002: 'Lunch',
            100003: 'Dinner',
            100004: 'Morning snack',
            100005: 'Afternoon snack',
            100006: 'Evening snack'
        }
        df['meal_type'] = pd.to_numeric(df['meal_type'], errors='coerce').map(meal_type_mapping)

    # Respiratory Rate Outlier Mapping
    if 'is_outlier' in df.columns and pd.api.types.is_numeric_dtype(df['is_outlier']):
        outlier_mapping = {
            0: 'valid',
            1: 'outlier'
        }
        df['is_outlier'] = pd.to_numeric(df['is_outlier'], errors='coerce').map(outlier_mapping)
        df.dropna(subset=['is_outlier'], inplace=True)

    # --- DATE CONVERSION ---
    if 'create_time' in df.columns:
        # Conversion with millisecond format
        df['create_time'] = pd.to_datetime(df['create_time'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

        # Remove invalid rows
        df = df.dropna(subset=['create_time'])
        df = df.sort_values('create_time')

        # --- SIDEBAR / FILTER ---
        st.sidebar.header("🛠️ Filters & Settings")

        # 1. Date Filter
        min_date = df['create_time'].min().to_pydatetime()
        max_date = df['create_time'].max().to_pydatetime()

        selected_dates = st.sidebar.slider(
            "Select time range",
            min_date, max_date,
            (min_date, max_date),
            format="DD.MM.YY"
        )

        # Apply filter
        mask = (df['create_time'] >= selected_dates[0]) & (df['create_time'] <= selected_dates[1])
        filtered_df = df.loc[mask].copy()

        # 2. Night Mode Filter
        st.sidebar.markdown("---")
        st.sidebar.subheader("🌙 Night Mode")
        use_night_filter = st.sidebar.checkbox("Restrict time window", value=False)

        if use_night_filter:
            start_t = st.sidebar.time_input("From", datetime.time(21, 0))
            end_t = st.sidebar.time_input("To", datetime.time(4, 30))


            def is_in_range(t):
                if start_t <= end_t:
                    return start_t <= t <= end_t
                else:  # Across midnight (e.g., 21:00 - 04:30)
                    return t >= start_t or t <= end_t

            filtered_df['temp_time'] = filtered_df['create_time'].dt.time
            filtered_df = filtered_df[filtered_df['temp_time'].apply(is_in_range)]

        # --- VISUALIZATION ---
        if not filtered_df.empty:
            # Automatic selection of numerical columns (measurement values)
            num_cols = filtered_df.select_dtypes(include=['number']).columns.tolist()
            # Remove IDs and unusable columns
            blacklist = ['tag_id', 'source', 'coverage_rate', 'client_data_ver', 'custom']
            clean_cols = [c for c in num_cols if c not in blacklist]

            # Add mapped categorical columns to the list of plottable columns
            all_string_cols = filtered_df.select_dtypes(include=['object', 'category']).columns.tolist()
            categorical_cols = [c for c in all_string_cols if c not in ['Source']]
            for col in reversed(categorical_cols):
                if col not in clean_cols:
                    clean_cols.insert(0, col)

            st.subheader("Graphical Analysis")
            y_axis = st.multiselect("Which values to display?", clean_cols, default=clean_cols[:1])

            if y_axis:
                # Reshape data for better plotting with Plotly Express
                plot_df = filtered_df.melt(
                    id_vars=['create_time', 'Source'],
                    value_vars=y_axis,
                    var_name='Metric',
                    value_name='Value'
                )
                fig = px.scatter(
                    plot_df,
                    x='create_time',
                    y='Value',
                    color='Source' if len(uploaded_files) > 1 else 'Metric',
                    symbol='Metric',
                    template="plotly_dark",
                )

                # Prevents ugly lines due to data gaps (e.g., during the day)
                fig.update_traces(connectgaps=False)

                # --- POLYNOMIAL INTERPOLATION ---
                st.sidebar.markdown("---")
                st.sidebar.subheader("📈 Trend Line")
                show_trend = st.sidebar.checkbox("Show interpolation", value=False)
                degree = st.sidebar.slider("Degree of polynomial", 1, 10, 3)

                if show_trend and len(filtered_df) > degree:
                    for col in y_axis:
                        # Convert time to numbers for calculation
                        x_numeric = pd.to_numeric(filtered_df['create_time'])
                        y_values = filtered_df[col]

                        # Calculate polynomial (Fit)
                        weights = np.polyfit(x_numeric, y_values, degree)
                        model = np.poly1d(weights)

                        # Calculate trend values
                        trend_line = model(x_numeric)

                        # Add trend line to chart
                        fig.add_scatter(
                            x=filtered_df['create_time'],
                            y=trend_line,
                            mode='lines',
                            name=f'Trend {col} (Degree {degree})',
                            line=dict(width=3)
                        )

                st.plotly_chart(fig, width="stretch")

            # --- STATISTICS ---
            st.markdown("---")

import pandas as pd
import glob
import argparse
from pathlib import Path

# Columns that appear in almost every Samsung Health file and should be removed
DEFAULT_DROP_COLS = [
    'create_sh_ver', 'modify_sh_ver', 'update_time', 'datauuid', 'pkg_name', 'deviceuuid',
    'client_data_ver', 'client_data_id', 'time_offset', 'comment', 'custom', 'data_version',
    'extra_data', 'binning_data', 'source', 'tag_id', 'day_time'
]

CLEANING_CONFIG = {
    "vitality": {
        "pattern": "com.samsung.shealth.vitality_score.*.csv",
        "output_name": "vitality_score.csv",
        "drop_cols": ['shr_calculation_index', 'shrv_calculation_index']
    },
    "sleep": {
        "pattern": "com.samsung.shealth.sleep.*.csv",
        "output_name": "sleep.csv",
        "drop_cols": ['total_sleep_time_weight', 'original_efficiency', 'has_sleep_data', 'combined_id',
                      'is_integrated', 'integrated_id', 'original_wake_up_time', 'original_bed_time']
    },
    "sleep_stage": {
        "pattern": "com.samsung.health.sleep_stage.*.csv",
        "output_name": "sleep_stage.csv",
        "drop_cols": ['sleep_id']
    },
    "oxygen_saturation": {
        "pattern": "com.samsung.shealth.tracker.oxygen_saturation.*.csv",
        "output_name": "oxygen_saturation.csv",
        "drop_cols": ['end_time', 'heart_rate', 'start_time', 'integrated_id', 'binning']
    },
    "heart_rate": {
        "pattern": "com.samsung.shealth.tracker.heart_rate.*.csv",
        "output_name": "heart_rate.csv",
        "drop_cols": ['heart_beat_count', 'start_time', 'end_time']
    },
    "pedometer_day_summary": {
        "pattern": "com.samsung.shealth.tracker.pedometer_day_summary.*.csv",
        "output_name": "pedometer_day_summary.csv",
        "drop_cols": ['source_package_name', 'source_info', 'achievement']
    },
    "weight": {
        "pattern": "com.samsung.health.weight.*.csv",
        "output_name": "weight.csv",
        "drop_cols": ['start_time', 'end_time']
    },
    "advanced_glycation_endproduct": {
        "pattern": "com.samsung.health.advanced_glycation_endproduct.*.csv",
        "output_name": "advanced_glycation_endproduct.csv",
        "drop_cols": ['start_time', 'end_time', 'level_boundary', 'version']
    },
    "antioxidant": {
        "pattern": "com.samsung.health.antioxidant.*.csv",
        "output_name": "antioxidant.csv",
        "drop_cols": ['start_time', 'end_time']
    },
    "ecg": {
        "pattern": "com.samsung.health.ecg.*.csv",
        "output_name": "ecg.csv",
        "drop_cols": ['start_time', 'end_time', 'ecg_version', 'sample_frequency', 'shm_data_id', 'shm_device_uuid',
                      'shm_update_time', 'chart_data', 'shm_create_time', 'ecg_version', 'data_mime', 'data',
                      'sample_count']
    },
    "floors_climbed": {
        "pattern": "com.samsung.health.floors_climbed.*.csv",
        "output_name": "floors_climbed.csv",
        "drop_cols": ['start_time', 'end_time', 'raw_data']
    },
    "food_info": {
        "pattern": "com.samsung.health.food_info.*.csv",
        "output_name": "food_info.csv",
        "drop_cols": ['start_time', 'end_time', 'description', 'provider_food_id', 'info_provider']
    },
    "food_intake": {
        "pattern": "com.samsung.health.food_intake.*.csv",
        "output_name": "food_intake.csv",
        "drop_cols": ['start_time', 'end_time', 'food_info_id']
    },
    "nutrition": {
        "pattern": "com.samsung.health.nutrition.*.csv",
        "output_name": "nutrition.csv",
        "drop_cols": ['start_time', 'end_time']
    },
    "respiratory_rate": {
        "pattern": "com.samsung.health.respiratory_rate.*.csv",
        "output_name": "respiratory_rate.csv",
        "drop_cols": ['start_time', 'end_time', 'pplib_version']
    },
    "skin_temperature": {
        "pattern": "com.samsung.health.skin_temperature.*.csv",
        "output_name": "skin_temperature.csv",
        "drop_cols": ['start_time', 'end_time']
    },
    "sleep_apnea": {
        "pattern": "com.samsung.health.sleep_apnea.*.csv",
        "output_name": "sleep_apnea.csv",
        "drop_cols": ['start_time', 'end_time', 'shm_start_time', 'shm_data_id', 'shm_device_id', 'start_time_offset',
                      'shm_create_time', 'shm_update_time', 'shm_device_uuid']
    },
    "user_profile": {
        "pattern": "com.samsung.health.user_profile.*.csv",
        "output_name": "user_profile.csv",
        "drop_cols": []
    },
    "activity_day_summary": {
        "pattern": "com.samsung.shealth.activity.day_summary.*.csv",
        "output_name": "activity_day_summary.csv",
        "drop_cols": ['start_time', 'end_time']
    }
}


def clean_health_data(base_dir):
    input_path = Path(base_dir)
    output_path = input_path / "cleaned"
    output_path.mkdir(exist_ok=True)

    for file_type, settings in CLEANING_CONFIG.items():
        search_pattern = str(input_path / settings["pattern"])
        files = glob.glob(search_pattern)

        if not files:
            continue

        all_dfs = []
        for file in files:
            df = pd.read_csv(file, index_col=False, skiprows=1, low_memory=False)

            # --- STEP 1: CLEAN COLUMNS (remove prefixes) ---
            # Turns 'com.samsung.health.heart_rate.bpm' into simply 'bpm'
            df.columns = [col.split('.')[-1] for col in df.columns]

            # --- STEP 2: APPLY EXCLUSION LIST ---
            # Now we can use simple names in drop_cols
            cols_to_drop = settings.get("drop_cols", []) + DEFAULT_DROP_COLS
            df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

            # --- Special transformation for sleep stages ---
            if file_type == "sleep_stage" and 'stage' in df.columns:
                sleep_stage_mapping = {
                    40001: 'Awake',
                    40002: 'Light sleep',
                    40003: 'Deep sleep',
                    40004: 'REM sleep'
                }
                df['stage'] = pd.to_numeric(df['stage'], errors='coerce')
                df['stage'] = df['stage'].map(sleep_stage_mapping)
                # Remove rows where mapping might have failed (e.g., NaN)
                df.dropna(subset=['stage'], inplace=True)

            # --- Special transformation for ECG symptoms ---
            if file_type == "ecg" and 'symptoms' in df.columns:
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
                        # Extracts the first number from something like '[1]' or '[1, 2]'
                        num_str = symptom_str.replace('[', '').replace(']', '').split(',')[0].strip()
                        return int(num_str) if num_str else 0
                    except (ValueError, IndexError):
                        return 0

                symptom_ids = df['symptoms'].apply(extract_symptom_id)
                df['symptoms'] = symptom_ids.map(symptom_mapping)

            # --- Special transformation for ECG classification ---
            if file_type == "ecg" and 'classification' in df.columns:
                classification_mapping = {
                    1: 'Sinus rhythm',
                    2: 'Atrial fibrillation',
                    3: 'Inconclusive',
                    4: 'Poor recording'
                }
                df['classification'] = pd.to_numeric(df['classification'], errors='coerce')
                df.dropna(subset=['classification'], inplace=True)
                df['classification'] = df['classification'].astype(int).map(classification_mapping)
                df.dropna(subset=['classification'], inplace=True)

            # --- Special transformation for Food Intake meal_type ---
            if file_type == "food_intake" and 'meal_type' in df.columns:
                meal_type_mapping = {
                    100001: 'Breakfast',
                    100002: 'Lunch',
                    100003: 'Dinner',
                    100004: 'Morning snack',
                    100005: 'Afternoon snack',
                    100006: 'Evening snack'
                }
                df['meal_type'] = pd.to_numeric(df['meal_type'], errors='coerce')
                df['meal_type'] = df['meal_type'].map(meal_type_mapping)

            # --- STEP 4: REORDER COLUMNS ---
            # Order by create_time, start_time, end_time, then the rest
            all_cols = list(df.columns)

            # Define the desired prefix order
            prefix_order = ['create_time', 'start_time', 'end_time']

            # Extract the prefix columns that exist in the DataFrame, in the correct order
            ordered_prefix = [col for col in prefix_order if col in all_cols]
            remaining_cols = [col for col in all_cols if col not in ordered_prefix]
            df = df[ordered_prefix + remaining_cols]

            all_dfs.append(df)

        if all_dfs:
            final_df: pd.DataFrame = pd.concat(all_dfs, ignore_index=True)
            target_file = output_path / settings["output_name"]
            final_df.to_csv(target_file, index=False, encoding='utf-8')
            print(f"[OK] {file_type} cleaned and unified.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder")
    args = parser.parse_args()
    clean_health_data(args.folder)

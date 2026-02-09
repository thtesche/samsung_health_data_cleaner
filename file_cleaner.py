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
        "drop_cols": ['start_time']
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

            # 3. Move create_time to the first position
            cols = list(df.columns)
            if 'create_time' in cols:
                cols.insert(0, cols.pop(cols.index('create_time')))
                df = df[cols]

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

import pandas as pd
import glob
import argparse
from pathlib import Path

CLEANING_CONFIG = {
    "vitality": {
        "pattern": "com.samsung.shealth.vitality_score.*.csv",
        "output_name": "vitality_score.csv",
        "drop_cols": ['create_sh_ver', 'modify_sh_ver', 'update_time', 'datauuid', 'pkg_name', 'deviceuuid',
                      'data_version', 'shr_calculation_index', 'shrv_calculation_index', 'day_time']
    },
    "sleep": {
        "pattern": "com.samsung.shealth.sleep.*.csv",
        "output_name": "sleep.csv",
        "drop_cols": ['datauuid', 'pkg_name', 'comment', 'deviceuuid', 'time_offset', 'client_data_ver',
                      'client_data_id', 'update_time', 'modify_sh_ver', 'total_sleep_time_weight',
                      'original_efficiency', 'has_sleep_data', 'combined_id', 'data_version', 'is_integrated',
                      'integrated_id', 'extra_data', 'create_sh_ver', 'custom', 'original_wake_up_time',
                      'original_bed_time']
    },
    "oxygen_saturation": {
        "pattern": "com.samsung.shealth.tracker.oxygen_saturation.*.csv",
        "output_name": "oxygen_saturation.csv",
        "drop_cols": ['source', 'comment', 'datauuid', 'pkg_name', 'update_time', 'deviceuuid', 'time_offset', 'tag_id',
                      'client_data_ver', 'client_data_id', 'end_time', 'data_version', 'heart_rate', 'start_time',
                      'modify_sh_ver', 'integrated_id', 'extra_data', 'create_sh_ver', 'custom', 'binning']
    },
    "heart_rate": {
        "pattern": "com.samsung.shealth.tracker.heart_rate.*.csv",
        "output_name": "heart_rate.csv",
        "drop_cols": ['create_sh_ver', 'modify_sh_ver', 'source', 'tag_id', 'datauuid', 'deviceuuid', 'pkg_name',
                      'update_time', 'time_offset', 'binning_data', 'heart_beat_count', 'custom',
                      'start_time', 'client_data_ver', 'client_data_id', 'comment', 'end_time']
    },
    "pedometer_day_summary": {
        "pattern": "com.samsung.shealth.tracker.pedometer_day_summary.*.csv",
        "output_name": "pedometer_day_summary.csv",
        "drop_cols": ['create_sh_ver', 'binning_data', 'modify_sh_ver', 'update_time', 'source_package_name', 'tag_id',
                      'source_info', 'deviceuuid', 'datauuid', 'pkg_name', 'time_offset', 'achievement', 'day_time']
    },
    "weight": {
        "pattern": "com.samsung.health.weight.*.csv",
        "output_name": "weight.csv",
        "drop_cols": ['create_sh_ver', 'start_time', 'binning_data', 'custom', 'modify_sh_ver', 'update_time',
                      'client_data_id', 'client_data_ver', 'time_offset', 'deviceuuid', 'comment', 'pkg_name',
                      'datauuid']
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
            df.drop(columns=settings["drop_cols"], inplace=True, errors='ignore')

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

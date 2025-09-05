import time
import os
from pathlib import Path
import importlib

# --- Dynamically import the processing modules ---
# This approach is used because the script names start with numbers,
# which are not valid Python identifiers for direct import statements.
step1_module_name = "post_processing.1_user_postprocessing"
step2_module_name = "post_processing.2_group_postprocessing"
step3_module_name = "post_processing.3_email_postprocessing"

try:
    user_postprocessing = importlib.import_module(step1_module_name)
    group_postprocessing = importlib.import_module(step2_module_name)
    email_postprocessing = importlib.import_module(step3_module_name)
except ImportError as e:
    print(f"Error: Could not import a processing module. Make sure you are running this script from the 'src' directory.")
    print(f"Details: {e}")
    exit(1)

def define_paths(output_dir: Path) -> dict:
    """
    Defines and returns a dictionary of all file paths used in the post-processing pipeline.
    This centralizes configuration and makes the pipeline easier to manage.
    """
    return {
        # Inputs from the main processing pipeline (to be deleted)
        "email_table": output_dir / "email_table.parquet",
        "user_table": output_dir / "user_table.parquet",
        "group_table": output_dir / "group_table.parquet",

        # Intermediary files from step 1 (to be deleted)
        "user_map_table": output_dir / "user_map_table.parquet",
        "to_delete_table": output_dir / "to_delete_table.parquet",

        # Intermediary file from step 2 (to be deleted)
        "group_remap": output_dir / "group_remap.parquet",

        # Final, cleaned tables (to be kept)
        "user_table_updated": output_dir / "user_table_updated.parquet",
        "groups_updated": output_dir / "groups_updated.parquet",
        "final_email_table": output_dir / "email_table_updated.parquet",
        "email_user_junction": output_dir / "email_user_junction.parquet",
        "email_group_junction": output_dir / "email_group_junction.parquet",
    }

def cleanup_intermediary_files(paths: dict):
    """
    Removes the intermediary files created during the pipeline, leaving only the final tables.
    """
    print("\n>>> Running Step 4: Cleaning up intermediary files...")
    
    files_to_remove = [
        "email_table",
        "user_table",
        "group_table",
        "user_map_table",
        "to_delete_table",
        "group_remap",
    ]

    for key in files_to_remove:
        file_path = paths.get(key)
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"  - Removed: {file_path.name}")
            except OSError as e:
                print(f"  - Error removing file {file_path}: {e}")
        else:
            print(f"  - Skipped (not found): {file_path.name if file_path else key}")
    
    print("<<< Cleanup complete. >>>")

def run_pipeline():
    """
    Orchestrates the entire post-processing pipeline, running each step in sequence.
    """
    print("--- Starting Post-Processing Pipeline ---")
    
    output_path = Path(__file__).parent.parent / "output"
    output_path.mkdir(exist_ok=True)
    paths = define_paths(output_path)

    # --- Step 1: User Deduplication and Merging ---
    print("\n>>> Running Step 1: User Post-Processing...")
    start_time = time.time()
    user_postprocessing.run(paths)
    print(f"<<< Step 1 finished in {time.time() - start_time:.2f} seconds. >>>")

    # --- Step 2: Group Consolidation ---
    print("\n>>> Running Step 2: Group Post-Processing...")
    start_time = time.time()
    group_postprocessing.run(paths)
    print(f"<<< Step 2 finished in {time.time() - start_time:.2f} seconds. >>>")

    # --- Step 3: Final Email Table Updates ---
    print("\n>>> Running Step 3: Email Post-Processing...")
    start_time = time.time()
    email_postprocessing.run(paths)
    print(f"<<< Step 3 finished in {time.time() - start_time:.2f} seconds. >>>")

    # --- Step 4: Cleanup ---
    cleanup_intermediary_files(paths)

    print("\n--- Post-Processing Pipeline Complete ---")
    print(f"Final output files are available in: {output_path.resolve()}")

if __name__ == "__main__":
    run_pipeline()

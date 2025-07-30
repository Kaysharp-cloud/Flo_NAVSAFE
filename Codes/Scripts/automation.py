import subprocess
import time
from datetime import datetime, timedelta

# Set how long the full process should run
start = datetime.now()
duration = timedelta(days=20)
end = start + duration

# Paths to Python scripts and executables
forecast_script = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\codes\get_nwm_sr_forecast.py"
fim_script = r"C:\Users\kbadebayo\Documents\ArcGIS\Projects\codes\FIM_generation.py"
arcgis_python = r"C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"

hour_count = 1

while datetime.now() < end:
    cycle_start = datetime.now()
    print(f"\nStarting cycle {hour_count} at {cycle_start.strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 1: Run the forecast downloader
    try:
        print("Step 1: Running discharge forecast download...")
        forecast_proc = subprocess.run(
            [r"C:\Users\kbadebayo\.venv\Scripts\python.exe", forecast_script],
            capture_output=True, text=True
        )
        print(forecast_proc.stdout)
        if forecast_proc.returncode != 0:
            print("Forecast download failed.")
            print(forecast_proc.stderr)
        else:
            print("Forecast download complete.")
    except Exception as e:
        print(f"Forecast step crashed: {e}")

    # Step 2: Wait 4 minutes before FIM generation
    print("Waiting 4 minutes before running ArcPy FIM...")
    time.sleep(4 * 60)

    # Step 3: Run the ArcPy-based FIM generator
    try:
        print("Step 2: Running FIM generation...")
        fim_proc = subprocess.run(
            [arcgis_python, fim_script],
            capture_output=True, text=True
        )
        print(fim_proc.stdout)
        if fim_proc.returncode != 0:
            print("FIM generation failed.")
            print(fim_proc.stderr)
        else:
            print("FIM generation complete.")
    except Exception as e:
        print(f"FIM step crashed: {e}")

    # Step 4: Wait until 1 hour from cycle start
    next_cycle_time = cycle_start + timedelta(hours=1)
    now = datetime.now()
    wait_seconds = (next_cycle_time - now).total_seconds()

    if wait_seconds > 0:
        print(f"Waiting {int(wait_seconds)} seconds until next cycle...\n")
        print(f"Waiting {int(wait_seconds)/60} minutes until next cycle...\n")
        time.sleep(wait_seconds)
    else:
        print("Cycle took longer than 1 hour. Starting next cycle immediately.\n")

    hour_count += 1

print("All cycles completed.")

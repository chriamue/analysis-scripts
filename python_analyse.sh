#!/bin/bash
for script in 66_drop_tables.py 01_init_db.py 02_upload_geo_data.py 03_download_report.py 04_reload_db_from_json.py 05_script_analysis.py
do
  python analysis/scripts/$script
done
echo "Writing merge_all_days.csv"
python -m analysis.utils.write_location_data

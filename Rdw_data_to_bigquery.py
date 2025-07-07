#!/usr/bin/env python3
# rdw_pipeline.py

import requests
import json
import datetime
import tempfile
import os
from google.cloud import storage
from google.cloud import bigquery

# ------------------------------
# CONFIG
# ------------------------------

project_id = "your-gcp-project"
bucket_name = "rdw_data_raw"
dataset_id = "rdw_data"
table_id = "vehicles_data"
temp_blob_name = "flattened_combined.json"

# Set this to True for a full load from 2023 onwards
# False = only previous month
full_load = False

# ------------------------------
# DATE RANGES
# ------------------------------

if full_load:
    start_date = "2023-01-01T00:00:00.000"
    end_date = None
else:
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    first_of_prev_month = (first_of_this_month - datetime.timedelta(days=1)).replace(day=1)

    start_date = first_of_prev_month.isoformat() + "T00:00:00.000"
    end_date = first_of_this_month.isoformat() + "T00:00:00.000"

print(f"Start date: {start_date}")
print(f"End date: {end_date}")

# ------------------------------
# FETCH FROM RDW
# ------------------------------

base_url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json"

where_clause = f"datum_eerste_toelating_dt >= '{start_date}'"
if end_date:
    where_clause += f" AND datum_eerste_toelating_dt < '{end_date}'"

params = {
    "$where": where_clause,
    "$limit": 1000,
    "$offset": 0,
}

all_data = []
while True:
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    if not data:
        break
    all_data.extend(data)
    params["$offset"] += 1000
    print(f"Fetched rows so far: {len(all_data)}")

print(f"Total rows fetched: {len(all_data)}")

# ------------------------------
# FILTER FIELDS
# ------------------------------

allowed_fields = {
    "kenteken",
    "voertuigsoort",
    "merk",
    "handelsbenaming",
    "variant",
    "datum_tenaamstelling",
    "inrichting",
    "aantal_zitplaatsen",
    "eerste_kleur",
    "tweede_kleur",
    "aantal_cilinders",
    "cilinderinhoud",
    "massa_ledig_voertuig",
    "massa_rijklaar",
    "datum_eerste_toelating",
    "datum_eerste_tenaamstelling_in_nederland",
    "brandstof_omschrijving",
    "maximum_snelheid",
    "lengte",
    "breedte",
    "wielbasis",
    "export_indicator",
    "openstaande_terugroepactie_indicator",
    "taxi_indicator",
    "maximum_massa_samenstelling",
    "tellerstandoordeel",
    "tenaamstellen_mogelijk",
    "datum_tenaamstelling_dt",
    "datum_eerste_toelating_dt",
    "maximum_last_onder_de_vooras_sen_tezamen_koppeling",
}

filtered_data = []
today_str = datetime.date.today().isoformat()

for row in all_data:
    filtered_row = {k: v for k, v in row.items() if k in allowed_fields}
    filtered_row["appended_date"] = today_str
    filtered_data.append(filtered_row)

print(f"Filtered rows: {len(filtered_data)}")

# ------------------------------
# WRITE NDJSON
# ------------------------------

with tempfile.NamedTemporaryFile("w", delete=False) as temp_file:
    for row in filtered_data:
        temp_file.write(json.dumps(row))
        temp_file.write("\n")
    temp_file_path = temp_file.name

print(f"NDJSON file written: {temp_file_path}")

# ------------------------------
# UPLOAD NDJSON TO GCS
# ------------------------------

storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)
temp_blob = bucket.blob(temp_blob_name)
temp_blob.upload_from_filename(temp_file_path)

print("NDJSON uploaded to GCS.")

# ------------------------------
# LOAD INTO BIGQUERY
# ------------------------------

bq_client = bigquery.Client(project=project_id)

uri = f"gs://{bucket_name}/{temp_blob_name}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
    write_disposition="WRITE_APPEND",
)

table_ref = f"{project_id}.{dataset_id}.{table_id}"

load_job = bq_client.load_table_from_uri(
    uri,
    table_ref,
    job_config=job_config,
)

load_job.result()

print("Data loaded into BigQuery.")

# ------------------------------
# CLEANUP
# ------------------------------

temp_blob.delete()
print("Temporary blob deleted from GCS.")

# Delete local temp file
os.remove(temp_file_path)
print("Temporary local file deleted.")

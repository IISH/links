#!/bin/sh
envsubst < "hsn-links-db-template.yaml" > "hsn-links-db.yaml"
envsubst < "export_csv-template.yaml" > "export_csv.yaml"
exec /app/export_csv.py

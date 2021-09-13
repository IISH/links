#!/bin/sh
envsubst < "hsn-links-db-template.yaml" > "hsn-links-db.yaml"
envsubst < "cleaned2rdf-template.yaml" > "cleaned2rdf.yaml"
exec /app/cleaned2rdf.py

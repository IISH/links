#!/bin/sh
envsubst < "links-template.properties" > "links.properties"
exec java -cp /app -Dproperties.path=/app/links.properties modulemain.LinksClean

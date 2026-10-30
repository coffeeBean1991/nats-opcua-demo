#!/bin/sh
# Run inside nats-box: wait for the local JetStream to answer, then create
# (or update) the stream described by $1. NATS_URL picks the server.
set -e
config="$1"
name=$(sed -n 's/.*"name": *"\([^"]*\)".*/\1/p' "$config" | head -n1)

until nats account info >/dev/null 2>&1; do
  echo "waiting for JetStream at $NATS_URL"
  sleep 1
done

if nats stream info "$name" >/dev/null 2>&1; then
  nats stream edit "$name" --config "$config" --force
else
  nats stream add --config "$config"
fi
nats stream info "$name"

# Stay up so the box can be used interactively: docker exec -it <box> sh
exec tail -f /dev/null

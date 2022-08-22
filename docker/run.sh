#!/bin/bash

if [ -z "${INFLUXDB_HOST}" ]; then
  INFLUXDB_HOST="influxdb"
fi

if [ -z "${INFLUXDB_PORT}" ]; then
  INFLUXDB_PORT="8086"
fi

if [ -z "${INFLUXDB_USER}" ]; then
  INFLUXDB_USER="aprs2influxdb"
fi

if [ -z "${INFLUXDB_PASSWORD}" ]; then
  INFLUXDB_PASSWORD="aprs2influxdb"
fi

if [ -z "${INFLUXDB_NAME}" ]; then
  INFLUXDB_NAME="aprs2influxdb"
fi

if [ -z "${CALLSIGN}" ]; then
  echo "Invalid callsign: \"${CALLSIGN}\""
  exit 1
fi

if [ -z "${PORT}" ]; then
  PORT="10152"
fi

if [ -z "${INTERVAL}" ]; then
  INTERVAL="15"
fi

APRS2INFLUXDB_CMD_DEBUG=""
if [ "${DEBUG}" = "true" ]; then
  APRS2INFLUXDB_CMD_DEBUG="--debug"
fi

/venv/bin/python3 \
  /app/__main__.py \
    --dbhost="${INFLUXDB_HOST}" \
    --dbport="${INFLUXDB_PORT}" \
    --dbuser="${INFLUXDB_USER}" \
    --dbpassword="${INFLUXDB_PASSWORD}" \
    --dbname="${INFLUXDB_NAME}" \
    --callsign="${CALLSIGN}" \
    --port="${PORT}" \
    --interval="${INTERVAL}" \
    ${APRS2INFLUXDB_CMD_DEBUG}

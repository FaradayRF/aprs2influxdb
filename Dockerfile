FROM python:3.10-alpine AS os

FROM os AS venv
COPY requirements.txt /
RUN python3 -m venv /venv \
    && /venv/bin/pip3 install --upgrade pip setuptools \
    && /venv/bin/pip3 install -r /requirements.txt

FROM os AS prod
COPY --from=venv /venv /venv
COPY aprs2influxdb /app
COPY docker/run.sh /
RUN addgroup -g 1000 aprs2influxdb \
    && adduser -h /home/aprs2influxdb -s /bin/sh -S -D -u 1000 aprs2influxdb \
    && mkdir -p /home/aprs2influxdb \
    && chown 1000:1000 /home/aprs2influxdb
WORKDIR /home/aprs2influxdb
USER 1000:1000
ENTRYPOINT ["/bin/sh", "/run.sh"]
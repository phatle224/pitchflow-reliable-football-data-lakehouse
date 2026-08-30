FROM apache/spark:3.5.3

USER root
RUN python3 -m pip install --no-cache-dir delta-spark==3.2.0 psycopg[binary]==3.2.3
USER spark

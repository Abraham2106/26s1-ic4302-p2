FROM apache/superset:latest

USER root
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir trino

COPY --chown=superset:superset init/ /app/superset/init/
RUN chmod +x /app/superset/init/superset-init.sh

USER superset
ENTRYPOINT [ "/app/superset/init/superset-init.sh" ]

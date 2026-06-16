FROM apache/spark:4.1.2-scala2.13-java17-python3-r-ubuntu

USER root
ENV HOME=/tmp

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

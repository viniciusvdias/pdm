FROM ghcr.io/dask/dask-notebook:latest

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      cmake \
      wget \
      unzip \
 && rm -rf /var/lib/apt/lists/*

USER jovyan
WORKDIR /home/jovyan

RUN pip install --no-cache-dir \
      dask-ml \
      lightgbm \
      scikit-learn \
      pandas \
      numpy \
      matplotlib \
      plotly

RUN mkdir -p /home/jovyan/work
WORKDIR /home/jovyan/work

EXPOSE 8888 8786 8787


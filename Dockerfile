ARG CUDA_VERSION=10.2
ARG LINUX_VERSION=ubuntu16.04
FROM rapidsai/rapidsai:cuda${CUDA_VERSION}-runtime-${LINUX_VERSION}

WORKDIR /rapids/
RUN mkdir census_demo

WORKDIR /rapids/spatial_demo
RUN mkdir data
WORKDIR /rapids/spatial_demo/data
RUN curl https://s3.us-east-2.amazonaws.com/rapidsai-data/viz-data/census_data.parquet.tar.gz -o census_data.parquet.tar.gz
RUN tar -xvzf census_data.parquet.tar.gz
RUN curl https://rapidsai-data.s3.amazonaws.com/viz-data/street-graph-us.tar.xz -o street-graph-us.tar.xz
RUN tar -xvzf street-graph-us.tar.xz



WORKDIR /rapids/spatial_demo

COPY . .

RUN source activate rapids && conda install -c conda-forge --file environment_for_docker.yml

ENTRYPOINT ["bash","./entrypoint.sh"]

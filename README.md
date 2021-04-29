# Drive-Time Spatial Analytics Viz
Using RAPIDS cuDF, cuSpatial, and cuGraph with Plotly Dash - compute the demographic information of 300M+ individuals within a drive-time isocrone within seconds for any point in the contiental US. 

![](./spatial-demo.png)

# Installation and Run Steps
## Base Layer Setup
The visualization uses a Jawg map base layer that requires an access token. Create one for free [here on jawg.io](https://www.jawg.io/lab/access-tokens). Go to the demo root directory's `plotly_demo` folder and create a token file named `.jawg_token`. Copy your token contents into the file.

**NOTE:** Without the token, application will fall back to the free open-street-map tiles.

## Datasets
The graph data for us-streets(nodes and edges), and the census data is automatically downloaded and extracted on the first run.


## Running the Visualization App
You can setup and run the visualization with the conda or docker commands below. Once the app is started, it will look for the demongraphics datasets locally and, if not found, will download it. Currently the street data needs to me be manually downloaded. 


## Data 
There are 3 main datasets:

- 2010 Census for Population Density, combined with the 2010 ACS for demographics  (~2.9 GB) | downloaded on first run
- US-street data - nodes (~4.4 GB) | downloaded on first run
- US-street data - edges (highways) (~3.3 GB) | downloaded on first run

For more information on how the Census and ACS data was prepared to show individual points, refer to the `/Data Processing` Directory.

### Conda Env

Verify the following arguments in the `environment.yml` match your system. An easy way to check is the `nvidia-smi` command. An easy way to check is the `nvidia-smi` command. The most up to date OS and CUDA versions supported can be found here: [RAPIDS requirements](https://rapids.ai/start.html#req)


```bash
# setup conda environment 
conda env create --name rapids_spatial_viz --file environment.yml
source activate rapids_spatial_viz

# run and access
cd plotly_demo
python app.py
```


### Docker

Verify the following arguments in the Dockerfile match your system. An easy way to check is the `nvidia-smi` command. The most up to date OS and CUDA versions supported can be found here: [RAPIDS requirements](https://rapids.ai/start.html#req)

```bash
# build
docker build -t plotly_demo .

# run and access via: http://localhost:8050 / http://ip_address:8050 / http://0.0.0.0:8050
docker run --gpus all -d -p 8050:8050 plotly_demo
```

## Dependencies

  - python=3.8
  - plotly=4.5
  - cudf
  - cuspatial
  - cugraph
  - dash=1.8
  - dash-leaflet=0.1.10
  - dask=2.30.0
  - dask-cuda
  - dask-cudf
  - dash-daq=0.3.2
  - dash-extensions=0.0.33
  - dash_bootstrap_components=0.10.7
  - jupyter-dash=0.3.1
  - pyproj=2.6.1
  - pandas=1.1.3
  - geopandas=0.8.1


## FAQ and Known Issues

**What hardware do I need to run this locally?** To run you need an NVIDIA GPU with at least 32GB of memory, and a Linux OS as defined in the [RAPIDS requirements](https://rapids.ai/start.html#req).

**Why is the population data from 2010?** Only census data is recorded on a block level, which provides the highest resolution population distributions available. For more details on census boundaries refer to the [TIGERweb app](https://tigerweb.geo.census.gov/tigerwebmain/TIGERweb_apps.html). 

**The dashboard stop responding or the chart data disappeared!** This is likely caused by an Out of Memory Error and the application must be restarted. 

**How do I request a feature or report a bug?** Create an [Issue](https://github.com/AjayThorve/Spatial-Analytics-Viz/issues) and we will get to it asap. 


## Acknowledgments and Data Sources

- 2010 Population Census and 2018 ACS data used with permission from IPUMS NHGIS, University of Minnesota, [www.nhgis.org](https://www.nhgis.org/) ( not for redistribution )
- Base map layer provided by [Jawg](https://www.jawg.io/)
- Dashboard developed with [Plot.ly Dash](https://plotly.com/dash/)

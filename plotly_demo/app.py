# -*- coding: utf-8 -*-
import cupy
import cudf
import dash_leaflet as dl
import dask_cudf
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import json
import numpy as np
import os
import pandas as pd
import plotly.express as px
import requests
import time
import tarfile

from dash_extensions.javascript import Namespace
from dash.dependencies import Input, Output
from plotly.colors import sequential
from pyproj import Transformer
from dask import delayed

from utils import get_nearest_polygons_from_selected_point

# Disable cupy memory pool so that cupy immediately releases GPU memory
cupy.cuda.set_allocator(None)

# Colors
bgcolor = "#191a1a"  # mapbox dark map land color
text_color = "#cfd8dc"  # Material blue-grey 100
mapbox_land_color = "#343332"

# Figure template
row_heights = [150, 440, 250, 210]
template = {
    'layout': {
        'paper_bgcolor': bgcolor,
        'plot_bgcolor': bgcolor,
        'font': {'color': text_color},
        "margin": {"r": 0, "t": 30, "l": 0, "b": 20},
        'bargap': 0.05,
        'xaxis': {'showgrid': False, 'automargin': True},
        'yaxis': {'showgrid': True, 'automargin': True},
        #   'gridwidth': 0.5, 'gridcolor': mapbox_land_color},
    }
}

colors = {}
colors['sex'] = ['#0d47a1', '#80e878']

mappings = {}
mappings_hover = {}
# Load mapbox token from environment variable or file
token = os.getenv('JAWG_TOKEN')
if not token:
    try:
        token = open(".jawg_token").read()
    except Exception as e:
        print('jawg token not found, using open-street-maps')

mappings_hover['cow'] = {
    0: "Private for-profit wage and salary workers: Employee of private company workers",
    1: "Private for-profit wage and salary workers: Self-employed in own incorporated business workers",
    2: "Private not-for-profit wage and salary workers",
    3: "Local government workers",
    4: "State government workers",
    5: "Federal government workers",
    6: "Self-employed in own not incorporated business workers",
    7: "Unpaid family workers",
    8: "Data not available/under 16 years",
}

mappings['cow'] = {
    0: "Emp",
    1: "Self-emp",
    2: "Emp non-profit",
    3: "Local gov emp",
    4: "State gov emp",
    5: "Federal gov emp",
    6: "Self-emp non-business",
    7: "Unpaid workers",
    8: "below age 16",
}

mappings['sex'] = {
    -1: "All genders",
    0: 'Males',
    1: 'Females'
}

mappings_hover['education'] = {
    0: "No schooling completed",
    1: "Nursery to 4th grade",
    2: "5th and 6th grade",
    3: "7th and 8th grade",
    4: "9th grade",
    5: "10th grade",
    6: "11th grade",
    7: "12th grade, no diploma",
    8: "High school graduate, GED, or alternative",
    9: "Some college, less than 1 year",
    10: "Some college, 1 or more years, no degree",
    11: "Associate's degree",
    12: "Bachelor's degree",
    13: "Master's degree",
    14: "Professional school degree",
    15: "Doctorate degree",
    16: 'below 16 years/data not available'
}


mappings['education'] = {
    0: "No school",
    1: "Upto 4th",
    2: "5th & 6th",
    3: "7th & 8th",
    4: "9th",
    5: "10th",
    6: "11th",
    7: "12th",
    8: "High school",
    9: "College(<1 yr)",
    10: "College(no degree)",
    11: "Associate's",
    12: "Bachelor's",
    13: "Master's",
    14: "Prof. school",
    15: "Doctorate",
    16: 'below age 16'
}


mappings_hover['income'] = {
    0: "$1 to $2,499 or loss",
    1: "$2,500 to $4,999",
    2: "$5,000 to $7,499",
    3: "$7,500 to $9,999",
    4: "$10,000 to $12,499",
    5: "$12,500 to $14,999",
    6: "$15,000 to $17,499",
    7: "$17,500 to $19,999",
    8: "$20,000 to $22,499",
    9: "$22,500 to $24,999",
    10: "$25,000 to $29,999",
    11: "$30,000 to $34,999",
    12: "$35,000 to $39,999",
    13: "$40,000 to $44,999",
    14: "$45,000 to $49,999",
    15: "$50,000 to $54,999",
    16: "$55,000 to $64,999",
    17: "$65,000 to $74,999",
    18: "$75,000 to $99,999",
    19: "$100,000 or more",
    20: 'below 25 years/unknown',
}

mappings['income'] = {
    0: "$2,499",
    1: "$4,999",
    2: "$7,499",
    3: "$9,999",
    4: "$12,499",
    5: "$14,999",
    6: "$17,499",
    7: "$19,999",
    8: "$22,499",
    9: "$24,999",
    10: "$29,999",
    11: "$34,999",
    12: "$39,999",
    13: "$44,999",
    14: "$49,999",
    15: "$54,999",
    16: "$64,999",
    17: "$74,999",
    18: "$99,999",
    19: "$100,000+",
    20: 'below age 25',
}


data_center_3857, data_3857, data_4326, data_center_4326 = [], [], [], []


def set_projection_bounds(df_d):
    transformer_4326_to_3857 = Transformer.from_crs("epsg:4326", "epsg:3857")

    def epsg_4326_to_3857(coords):
        return [transformer_4326_to_3857.transform(*reversed(row)) for row in coords]

    transformer_3857_to_4326 = Transformer.from_crs("epsg:3857", "epsg:4326")

    def epsg_3857_to_4326(coords):
        return [list(reversed(transformer_3857_to_4326.transform(*row))) for row in coords]

    data_3857 = (
        [df_d.x.min(), df_d.y.min()],
        [df_d.x.max(), df_d.y.max()]
    )
    data_center_3857 = [[
        (data_3857[0][0] + data_3857[1][0]) / 2.0,
        (data_3857[0][1] + data_3857[1][1]) / 2.0,
    ]]

    data_4326 = epsg_3857_to_4326(data_3857)
    data_center_4326 = epsg_3857_to_4326(data_center_3857)

    return data_3857, data_center_3857, data_4326, data_center_4326

# Build Dash app and initial layout


def blank_fig(height):
    """
    Build blank figure with the requested height
    Args:
        height: height of blank figure in pixels
    Returns:
        Figure dict
    """
    return {
        'data': [],
        'layout': {
            'height': height,
            'template': template,
            'xaxis': {'visible': False},
            'yaxis': {'visible': False},
        }
    }


average_speeds = [25, 35, 45, 60]
trip_times = [5, 10, 15, 30, 60]

ns = Namespace("dlx", "choropleth")
classes = [0, 1, 2, 3, 4, 5]
colorscale = ['white']
style = dict(weight=2, opacity=1, color='white', dashArray='3', fillOpacity=0.2)
minmax = dict(min=0, max=1)
chroma = "https://cdnjs.cloudflare.com/ajax/libs/chroma-js/2.1.0/chroma.min.js"


if not token:
    keys = ["watercolor", "toner", "terrain"]
    url_template = "http://{{s}}.tile.stamen.com/{}/{{z}}/{{x}}/{{y}}.png"
    attribution = 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, ' \
                '<a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data ' \
                '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    checked_key = "toner"
else:
    keys = ["sunny", "matrix", "dark", "terrain", "streets"]
    url_template = "https://{{s}}.tile.jawg.io/jawg-{}/{{z}}/{{x}}/{{y}}{{r}}.png?access-token="+token
    attribution = '<a href="http://jawg.io" title="Tiles Courtesy of Jawg Maps" target="_blank">&copy; <b>Jawg</b>Maps</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    checked_key = "matrix"


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], external_scripts=[chroma],)
app.layout = html.Div(children=[
    html.Div([
        html.H1(children=[
            'Census 2010 + Spatial Querying Dashboard',
            html.A(
                html.Img(
                    src="assets/rapids-logo.png",
                    style={'float': 'right', 'height': '45px',
                           'margin-right': '1%', 'margin-top': '-7px'}
                ), href="https://rapids.ai/"),
            html.A(
                html.Img(
                    src="assets/dash-logo.png",
                    style={'float': 'right', 'height': '30px'}
                ), href="https://dash.plot.ly/"),

        ], style={'text-align': 'left'}),
    ]),
    html.Div(children=[
        html.Div(children=[
            html.Div(children=[
                html.H4([
                    "Population Count",
                ], className="container_title"),
                dcc.Loading([
                    dcc.Graph(
                        id='indicator-graph',
                        figure=blank_fig(row_heights[3]),
                        config={'displayModeBar': False},
                    ),
                ], style={'width': '100%'}),
            ], style={'height': f'{row_heights[3]}px', 'margin-right': '2%'}, className='four columns pretty_container', id="indicator-div"),
        ]),
        html.Div(children=[
            html.Div(children=[
                html.H4([
                    "Compute Time (seconds)",
                ], className="container_title"),
                dcc.Loading([
                    dcc.Graph(
                        id='query-time-pie',
                        figure=blank_fig(row_heights[3]),
                        config={'displayModeBar': False},
                    ),
                ], style={'width': '100%'}),
            ], style={'height': f'{row_heights[3]}px'}, className='eight columns pretty_container', id="pipeline-div"),
        ]),
        html.Div(children=[
            html.Button("Clear Selection", id='reset-map',
                        className='reset-button'),
            html.H4([
                "Population Distribution of Individuals",
            ], className="container_title"),
            html.Div([
                dl.Map([
                    dl.LayersControl(
                        [
                            dl.BaseLayer(dl.TileLayer(url=url_template.format(key), attribution=attribution),
                            name=key, checked=key == checked_key) for key in keys] +
                        [
                            dl.LayerGroup(id="layer"),
                            dl.GeoJSON(id='polygons', options=dict(style=ns("style")), hideout=dict(colorscale=colorscale, classes=classes, style=style, colorProp="index"),)
                        ]
                    ),
                ],
                id="map-graph", zoom=4, center=(39, -100)),
                html.Div([
                    html.H5("Average Speed", style={"color":"#191a1a"}),
                    dcc.Dropdown(
                        id='average-speed',
                        options=[{'label': i, 'value': i} for i in average_speeds],
                        value=35
                    ),
                    html.H5("Trip Time", style={"color":"#191a1a"}),
                    dcc.Dropdown(
                        id='trip-time',
                        options=[{'label': f"{i} minutes", 'value': i} for i in trip_times],
                        value=30,
                    )], className="info",
                    style={"position": "relative", "bottom": "190px", "left": "10px", "z-index": "1000", "width": "400px"}
                ),

        ], style={'width': '100%', 'height': '50vh', 'margin': "auto", "display": "block", "position": "relative"}),
            # Hidden div inside the app that stores the intermediate value
            html.Div(id='intermediate-state-value', style={'display': 'none'})

        ], className='twelve columns pretty_container',
            style={
                'width': '98%',
                'margin-right': '0',
        },
            id="map-div"
        ),
        html.Div(children=[
            html.Div(
                children=[
                    html.Button(
                        "Clear Selection", id='clear-education', className='reset-button'
                    ),
                    html.H4([
                        "Education Distribution",
                    ], className="container_title"),

                    dcc.Graph(
                        id='education-histogram',
                        config={'displayModeBar': False},
                        figure=blank_fig(row_heights[2]),
                        animate=False
                    ),
                ],
                style={'margin-right': '2%'}, className='six columns pretty_container', id="education-div"
            )
        ]),
        html.Div(children=[
            html.Div(
                children=[
                    html.Button(
                        "Clear Selection", id='clear-income', className='reset-button'
                    ),
                    html.H4([
                        "Income Distribution",
                    ], className="container_title"),

                    dcc.Graph(
                        id='income-histogram',
                        config={'displayModeBar': False},
                        figure=blank_fig(row_heights[2]),
                        animate=False
                    ),
                ],
                className='six columns pretty_container', id="income-div"
            )
        ]),
        html.Div(children=[
            html.Div(
                children=[
                    html.Button(
                        "Clear Selection", id='clear-cow', className='reset-button'
                    ),
                    html.H4([
                        "Class of Workers Distribution",
                    ], className="container_title"),

                    dcc.Graph(
                        id='cow-histogram',
                        config={'displayModeBar': False},
                        figure=blank_fig(row_heights[2]),
                        animate=False
                    ),
                ],
                style={'margin-right': '2%'}, className='six columns pretty_container', id="cow-div"
            )
        ]),
        html.Div(children=[
            html.Div(
                children=[
                    html.Button(
                        "Clear Selection", id='clear-age', className='reset-button'
                    ),
                    html.H4([
                        "Age Distribution",
                    ], className="container_title"),

                    dcc.Graph(
                        id='age-histogram',
                        config={'displayModeBar': False},
                        figure=blank_fig(row_heights[2]),
                        animate=False
                    ),
                ],
                className='six columns pretty_container', id="age-div"
            )
        ]),
    ]),
    html.Div(
        [
            html.H4('Acknowledgements and Data Sources',
                    style={"margin-top": "0"}),
            dcc.Markdown('''\
**Important Data Caveats:** Geospatially filtered data will show accurate distribution, but due to anonymized, multiple cross filtered distributions will not return meaningful results. See [FAQ](https://github.com/rapidsai/plotly-dash-rapids-census-demo/tree/master#faq-and-known-issues) fore details.
- 2010 Population Census and 2018 ACS data used with permission from IPUMS NHGIS, University of Minnesota, [www.nhgis.org](https://www.nhgis.org/) ( not for redistribution ).
- Base map layer provided by [Jawg](https://www.jawg.io/).
- Dashboard developed with [Plot.ly Dash](https://plotly.com/dash/).
- Geospatial point rendering developed with [dash-leaflet](https://dash-leaflet.herokuapp.com/).
- GPU accelerated with [RAPIDS cudf](https://rapids.ai/) and [cupy](https://cupy.chainer.org/)
- For source code and data workflow, visit our [GitHub](https://github.com/AjayThorve/Spatial-Analytics-Viz).
'''),
        ],
        style={
            'width': '98%',
            'margin-right': '0',
            'padding': '10px',
        },
        className='twelve columns pretty_container',
    ),
])


# Clear/reset button callbacks
@app.callback(
    Output('map-graph', 'selectedData'),
    [Input('reset-map', 'n_clicks'), Input('clear-all', 'n_clicks')]
)
def clear_map(*args):
    return None


@app.callback(
    Output('age-histogram', 'selectedData'),
    [Input('clear-age', 'n_clicks'), Input('clear-all', 'n_clicks')]
)
def clear_age_hist_selections(*args):
    return None


@app.callback(
    Output('education-histogram', 'selectedData'),
    [Input('clear-education', 'n_clicks'), Input('clear-all', 'n_clicks')]
)
def clear_education_hist_selections(*args):
    return None


@app.callback(
    Output('income-histogram', 'selectedData'),
    [Input('clear-income', 'n_clicks'), Input('clear-all', 'n_clicks')]
)
def clear_income_hist_selections(*args):
    return None


@app.callback(
    Output('cow-histogram', 'selectedData'),
    [Input('clear-cow', 'n_clicks'), Input('clear-all', 'n_clicks')]
)
def clear_cow_hist_selections(*args):
    return None

# Query string helpers


def bar_selection_to_query(selection, column):
    """
    Compute pandas query expression string for selection callback data

    Args:
        selection: selectedData dictionary from Dash callback on a bar trace
        column: Name of the column that the selected bar chart is based on

    Returns:
        String containing a query expression compatible with DataFrame.query. This
        expression will filter the input DataFrame to contain only those rows that
        are contained in the selection.
    """
    point_inds = [p['label'] for p in selection['points']]
    xmin = min(point_inds)  # bin_edges[min(point_inds)]
    xmax = max(point_inds) + 1  # bin_edges[max(point_inds) + 1]
    xmin_op = "<="
    xmax_op = "<="
    return f"{xmin} {xmin_op} {column} and {column} {xmax_op} {xmax}"


def build_query(selections, exclude=None):
    """
    Build pandas query expression string for cross-filtered plot

    Args:
        selections: Dictionary from column name to query expression
        exclude: If specified, column to exclude from combined expression

    Returns:
        String containing a query expression compatible with DataFrame.query.
    """
    other_selected = {sel for c, sel in selections.items() if (
        c != exclude and sel != -1)}
    if other_selected:
        return ' and '.join(other_selected)
    else:
        return None


# Plot functions
def build_colorscale(colorscale_name, transform):
    """
    Build plotly colorscale

    Args:
        colorscale_name: Name of a colorscale from the plotly.colors.sequential module
        transform: Transform to apply to colors scale. One of 'linear', 'sqrt', 'cbrt',
        or 'log'

    Returns:
        Plotly color scale list
    """
    global colors, mappings

    colors_temp = getattr(sequential, colorscale_name)
    if transform == "linear":
        scale_values = np.linspace(0, 1, len(colors_temp))
    elif transform == "sqrt":
        scale_values = np.linspace(0, 1, len(colors_temp)) ** 2
    elif transform == "cbrt":
        scale_values = np.linspace(0, 1, len(colors_temp)) ** 3
    elif transform == "log":
        scale_values = (10 ** np.linspace(0, 1, len(colors_temp)) - 1) / 9
    else:
        raise ValueError("Unexpected colorscale transform")
    return [(v, clr) for v, clr in zip(scale_values, colors_temp)]


def query_df_range(df, col, x0, x1):
    mask_ = (df[col] >= x0) & (df[col] < x1)
    if(mask_.sum() != len(df)):
        df = df[mask_]
        df.index = cudf.core.RangeIndex(0, len(df))
    del(mask_)
    return df


def query_df_range_lat_lon(df, x0, x1, y0, y1, x, y):
    mask_ = (df[x] >= x0) & (df[x] <= x1) & (df[y] <= y0) & (df[y] >= y1)
    if(mask_.sum() != len(df)):
        df = df[mask_]
        df.index = cudf.core.RangeIndex(0, len(df))
    del(mask_)
    return df


def build_histogram_default_bins(
    df, column,
    orientation, colorscale_name, colorscale_transform
):
    """
    Build histogram figure

    Args:
        df: pandas or cudf DataFrame
        column: Column name to build histogram from
    Returns:
        Histogram figure dictionary
    """
    if isinstance(df, cudf.DataFrame):
        df = df.groupby(column)['x'].count().to_pandas()
    else:
        df = df.groupby(column)['x'].count()

    bin_edges = df.index.values
    counts = df.values

    mapping_options = {}
    xaxis_labels = {}
    if column in mappings:
        if column in mappings_hover:
            mapping_options = {
                'text': list(mappings_hover[column].values()),
                'hovertemplate': "%{text}: %{y} <extra></extra>"
            }
        else:
            mapping_options = {
                'text': list(mappings[column].values()),
                'hovertemplate': "%{text} : %{y} <extra></extra>"
            }
        xaxis_labels = {
            'tickvals': list(mappings[column].keys()),
            'ticktext': list(mappings[column].values())
        }

    # color_scale = build_colorscale(colorscale_name, colorscale_transform)

    # centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    if orientation == 'h':
        fig = {
            'data': [{
                'type': 'bar', 'x': bin_edges, 'y': counts,
                'marker': {
                    'color': counts,
                    'colorscale': build_colorscale(colorscale_name, 'linear')
                },
                **mapping_options
            }],
            'layout': {
                'xaxis': {
                    'type': 'linear',
                    'range': [0, counts.max()],
                    'title': {
                        'text': "Count"
                    },
                },
                'yaxis': {
                    **xaxis_labels
                },
                'selectdirection': 'v',
                'dragmode': 'select',
                'template': template,
                'uirevision': True,
                'hovermode': 'closest'
            }
        }
    else:
        fig = {
            'data': [{
                'type': 'bar', 'x': bin_edges, 'y': counts,
                'marker': {
                    'color': counts,
                    'colorscale': build_colorscale(colorscale_name, 'linear')
                },
                **mapping_options

            }],
            'layout': {
                'yaxis': {
                    'type': 'linear',
                    'title': {
                        'text': "Count"
                    },
                },
                'xaxis': {
                    **xaxis_labels
                },
                'selectdirection': 'h',
                'dragmode': 'select',
                'template': template,
                'uirevision': True,
                'hovermode': 'closest'
            }
        }

    return fig


def build_updated_figures(
        df, colorscale_name
):
    """
    Build all figures for dashboard

    Args:
        - df: census 2010 dataset (cudf.DataFrame)
        - colorscale_name
    Returns:
        tuple of figures in the following order
        (datashader_plot, education_histogram, income_histogram,
        cow_histogram, age_histogram, n_selected_indicator,
        coordinates_4326_backup, position_backup)
    """
    colorscale_transform = 'linear'

    education_histogram = build_histogram_default_bins(
        df, 'education', 'v', colorscale_name, colorscale_transform
    )

    income_histogram = build_histogram_default_bins(
        df, 'income', 'v', colorscale_name, colorscale_transform
    )

    cow_histogram = build_histogram_default_bins(
        df, 'cow', 'v', colorscale_name, colorscale_transform
    )

    age_histogram = build_histogram_default_bins(
        df, 'age', 'v', colorscale_name, colorscale_transform
    )

    return (
        education_histogram, income_histogram,
        cow_histogram, age_histogram,
    )


def get_pie(times, colorscale_name):
    query_pipeline = [
        'filter us-street graph by a 30 mile radius',
        'compute nearest node to selected point',
        'compute shortests paths to all nodes',
        'filter polygons by trip times',
        'query census data'
    ]
    df = pd.DataFrame({'query_type': query_pipeline, 'query_time': times})
    df['idx'] = 0
    fig = px.bar(
        df, y='idx', x='query_time', color='query_type', orientation='h',
        color_discrete_sequence=getattr(sequential, colorscale_name),
        height=row_heights[3]-50, hover_name="query_type",
        template=template
    )
    fig.update_layout(hovermode='closest')
    fig.layout.yaxis.visible = False
    fig.layout.xaxis.title = 'seconds'
    fig.layout.legend.title = ''
    fig.layout.bargap = 0.3
    for i in fig.data:
        i.hovertemplate = '<b>%{hovertext}</b><br>query_time=%{x} seconds<extra></extra>'
    return fig


@app.callback(
    [
        Output('indicator-graph', 'figure'),
        Output('query-time-pie', 'figure'),
        Output("layer", "children"), Output("polygons", "data"),

        Output('education-histogram', 'figure'),
        Output('income-histogram', 'figure'),
        Output('cow-histogram', 'figure'),
        Output('age-histogram', 'figure'),

        Output('education-histogram', 'config'),
        Output('income-histogram', 'config'),
        Output('cow-histogram', 'config'),
        Output('age-histogram', 'config'),
    ],
    [
        Input("map-graph", "click_lat_lng"), Input('average-speed', 'value'),
        Input('trip-time', 'value'),
    ]
)
def update_plots(
        click_lat_lng, average_speed, trip_time
):
    global census_data, cudf_nodes, cudf_edges
    colorscale_name = 'Blugrn'
    t0 = time.time()

    if click_lat_lng is not None:
        lat, lon = click_lat_lng
        marker = dl.Marker(position=click_lat_lng, children=dl.Tooltip("({:.3f}, {:.3f})".format(*click_lat_lng)))

        polygons, df, times = get_nearest_polygons_from_selected_point(
            lat, lon, average_speed, trip_time, cudf_nodes, cudf_edges, census_data
        )
        polygon_data = json.loads(polygons.to_json())
    else:
        marker, polygon_data, df = None, None, None
        times = [0, 0, 0, 0]

    if df is None:
        len_df = len(census_data)
        figures = delayed(build_updated_figures)(census_data, colorscale_name).compute()

    else:
        len_df = len(df)
        figures = build_updated_figures(df, colorscale_name)

    (education_histogram, income_histogram,
     cow_histogram, age_histogram) = figures

    barchart_config = {
        'displayModeBar': True,

        'modeBarButtonsToRemove': [
            'zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d',
            'resetScale2d', 'hoverClosestCartesian', 'hoverCompareCartesian', 'toggleSpikelines'
        ]
    }
    n_selected_indicator = {
        'data': [{
            'domain': {
                'x': [0, 1], 'y': [0, 0.5]
            },
            'type': 'indicator',
            'value': len_df,
            'number': {
                'font': {
                    'color': text_color,
                    'size': '15px'
                },
                "valueformat": ","
            }
        }],
        'layout': {
            'template': template,
            'height': row_heights[0]-30,
            'margin': {'l': 10, 'r': 10, 't': 10, 'b': 10}
        }
    }
    compute_time = round(time.time() - t0, 4)
    print(f"Update time: {compute_time}")
    np.append(times, [compute_time - np.sum(times)])
    query_time_pie = get_pie(
        np.append(times, [compute_time - np.sum(times)]), colorscale_name
    )

    return (
        n_selected_indicator,
        query_time_pie,
        marker, polygon_data,
        education_histogram, income_histogram, cow_histogram, age_histogram,
        barchart_config, barchart_config, barchart_config, barchart_config
    )


def check_dataset(dataset_url, data_path):
    if not os.path.exists(data_path):
        print(f"Dataset not found at "+data_path+".\n"
              f"Downloading from {dataset_url}")
        # Download dataset to data directory
        os.makedirs('../data', exist_ok=True)
        data_gz_path = data_path.split('/*')[0] + '.tar.gz'
        with requests.get(dataset_url, stream=True) as r:
            r.raise_for_status()
            with open(data_gz_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        print("Decompressing...")
        f_in = tarfile.open(data_gz_path, 'r:gz')
        f_in.extractall('../data')

        print("Deleting compressed file...")
        os.remove(data_gz_path)

        print('done!')
    else:
        print(f"Found dataset at {data_path}")


census_data = None
cudf_nodes = None
cudf_edges = None


def load_datasets():
    global census_data, cudf_nodes, cudf_edges
    census_data_url = 'https://s3.us-east-2.amazonaws.com/rapidsai-data/viz-data/census_data.parquet.tar.gz'
    data_path = "../../plotly/plotly-dash-rapids-census-demo/data/census_data.parquet"
    check_dataset(census_data_url, data_path)

    nodes_path = "../data/us-nodes.parquet"
    edges_path = "../data/us-edges.parquet"

    # cudf DataFrame
    census_data = dask_cudf.read_parquet(data_path)
    cudf_nodes = cudf.read_parquet(nodes_path)
    cudf_edges = cudf.read_parquet(edges_path)


def server():
    # gunicorn entry point when called with `gunicorn 'app:server()'`
    load_datasets()
    return app.server


if __name__ == '__main__':
    # development entry point
    load_datasets()

    # Launch dashboard
    app.run_server(
        debug=False, dev_tools_silence_routes_logging=True, host='0.0.0.0')

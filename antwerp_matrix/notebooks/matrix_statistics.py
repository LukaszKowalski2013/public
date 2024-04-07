import pandas as pd
import os
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Set the working directory
os.chdir(r"E:\swiat_gis\freelancing projects\antwerp\antwerp_matrix\data")

# Load the data
micro_micro_gdf = pd.read_csv("micro_micro_with_xy.csv")
differences = pd.read_csv("differences.csv")
missed_flows = pd.read_csv("missed_flows.csv")
more_micro_than_total = pd.read_csv("more_micro_than_total.csv")
less_micro_than_total = pd.read_csv("less_micro_than_total.csv")

# total flows in antwerp matrix
total_flows_mean = 5124580
total_flows_ant = 4994773

# split all dfs by source into 4 groups
sources = differences["source"].unique()
source_dfs = [differences[differences["source"] == source] for source in sources]
# describe all by 3 vars: "diff", '[7]-Totaal', 'micro_micro_total"
descs = [source_df[["diff", '[7]-Totaal', 'micro_micro_total']].describe() for source_df in source_dfs]

# run the same statistics for missed_flows
missed_flows_sources = missed_flows["source"].unique()
missed_flows_source_dfs = [missed_flows[missed_flows["source"] == source] for source in missed_flows_sources]
missed_flows_descs = [source_df['[7]-Totaal'].describe() for source_df in missed_flows_source_dfs]
missed_flows_sums = [source_df['[7]-Totaal'].sum() for source_df in missed_flows_source_dfs]

# create df from missed_flows_descs add source info and sum at the end as new row
missed_flows = pd.concat(missed_flows_descs, axis=1)
missed_flows.columns = missed_flows_sources
missed_flows = missed_flows.T
missed_flows["sum"] = missed_flows_sums
missed_flows['source'] = missed_flows.index
missed_flows = missed_flows[['source', 'count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max', 'sum']]
missed_flows.to_csv("missed_flows_descs.csv")




# plot data
fig1 = px.box(differences, x="source", y="diff", title="Differences in flows between the total and micro-micro matrices")
fig1.write_html("boxplot_diff.html")

fig2 = px.scatter(differences, x="diff", y="micro_micro_total", color="source",
                  title="Differences in flows between the total and micro-micro matrices")
fig2.write_html("scatterplot_diff.html")

fig3 = px.scatter(differences, x="[7]-Totaal", y="micro_micro_total", color="source",
                  title="Differences in flows between the total and micro-micro matrices")
fig3.write_html("scatterplot_total_micro.html")

# histograms
fig4 = px.histogram(differences, x="diff", color="source", opacity=.21, nbins=50,
                    title="Histogram of differences in flows between the total and micro-micro matrices")
fig4.write_html("hist_diff.html")

# show descs in 4 subplots in the first row and missed_flows_descs in the second row - 4 subplots
def plot_4_describe_tables(sources, descs, title='', output_file="descs.html"):
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        specs=[[{"type": "table"}],
               [{"type": "table"}],
               [{"type": "table"}],
               [{"type": "table"}]],
        subplot_titles=(sources[0], sources[1], sources[2], sources[3])
    )
    df = descs[0]
    df['statistics'] = df.index
    df = df[['statistics', '[7]-Totaal', 'micro_micro_total', 'diff']]
    fig.add_trace(
        go.Table(
            header=dict(
                values=['statistics', 'diff', '[7]-Totaal', 'micro_micro_total'],
                font=dict(size=10),
                align="left"
            ),
            cells=dict(
                values=[df[k].tolist() for k in df.columns[0:]],
                align="left",
                # format numeric data
                format=[None, ".1f", ".1f", ".1f"],
            )
        ),
        row=1, col=1
    )
    df = descs[1]
    df['statistics'] = df.index
    df = df[['statistics', '[7]-Totaal', 'micro_micro_total', 'diff']]
    fig.add_trace(
        go.Table(
            header=dict(
                values=['statistics', 'diff', '[7]-Totaal', 'micro_micro_total'],
                font=dict(size=10),
                align="left"
            ),
            cells=dict(
                values=[df[k].tolist() for k in df.columns[0:]],
                align="left",
                # format numeric data
                format=[None, ".1f", ".1f", ".1f"],
            )
        ),
        row=2, col=1
    )
    df = descs[2]
    df['statistics'] = df.index
    df = df[['statistics', '[7]-Totaal', 'micro_micro_total', 'diff']]
    fig.add_trace(
        go.Table(
            header=dict(
                values=['statistics', 'diff', '[7]-Totaal', 'micro_micro_total'],
                font=dict(size=10),
                align="left"
            ),
            cells=dict(
                values=[df[k].tolist() for k in df.columns[0:]],
                align="left",
                # format numeric data
                format=[None, ".1f", ".1f", ".1f"],
            )
        ),
        row=3, col=1
    )
    df = descs[3]
    df['statistics'] = df.index
    df = df[['statistics', '[7]-Totaal', 'micro_micro_total', 'diff']]
    fig.add_trace(
        go.Table(
            header=dict(
                values=['statistics', 'diff', '[7]-Totaal', 'micro_micro_total'],
                font=dict(size=10),
                align="left"
            ),
            cells=dict(
                values=[df[k].tolist() for k in df.columns[0:]],
                align="left",
                # format numeric data
                format=[None, ".1f", ".1f", ".1f"],
            )
        ),
        row=4, col=1
    )
    fig.update_layout(
        height=1000,
        showlegend=False,
        title_text=title,
    )
    fig.write_html("descs.html")
    return fig

fig = plot_4_describe_tables(sources, descs, title = 'descriptive statistics for difference df',output_file="descs.html")

# plot missed flows df
df = missed_flows

fig = go.Figure(data=[go.Table(
    header=dict(values=list(df.columns),
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=[df[k].tolist() for k in df.columns[0:]],
               fill_color='lavender',
               align='left',
               format=[None, ".1f", ".1f", ".1f"]),
)
])
fig.write_html("missed_flows_descs.html")
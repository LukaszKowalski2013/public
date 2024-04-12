import pandas as pd
import geopandas as gpd
import os

os.chdir(r"E:\swiat_gis\freelancing projects\antwerp\antwerp_matrix\data")

# read in data
gdf_ANT = gpd.read_file("ANT_clipped_TAZ.geojson")
gdf_KEM = gpd.read_file("KEM_clipped_TAZ.geojson")
gdf_MEC = gpd.read_file("MEC_clipped_TAZ.geojson")
gdf_WAA = gpd.read_file("WAA_clipped_TAZ.geojson")
gdfs = pd.concat([gdf_ANT, gdf_KEM, gdf_MEC, gdf_WAA], axis=0)
gdfs.to_crs(epsg=4326, inplace=True)
# get centroids of all zones
gdfs['x'] = gdfs.centroid.x
gdfs['y'] = gdfs.centroid.y
# gdfs columns = 'ZONENUMMER', 'STGB', 'VREGIO_L', 'MATRIX', 'geometry', 'x', 'y'

df_ANT = pd.read_csv("ANT_clipped_MATRIX.csv")
df_ANT['source'] = 'ANT'
df_KEM = pd.read_csv("KEM_clipped_MATRIX.csv")
df_KEM['source'] = 'KEM'
df_MEC = pd.read_csv("MEC_clipped_MATRIX.csv")
df_MEC['source'] = 'MEC'
df_WAA = pd.read_csv("WAA_clipped_MATRIX.csv")
df_WAA['source'] = 'WAA'
# df columns: ['Unnamed: 0', 'i', 'j', '[1]-Bestuurder', '[2]-Passagier', '[3]-Trein',
#        '[4]-BTM', '[5]-Fiets', '[6]-Te Voet', '[7]-Totaal', 'source'],

m = gpd.read_file("union.geojson") # the file was created in QGIS by union of all input 4 geodataframes

## STEP 1 CALCULATE MICROREGIONS BY UNION OF ALL GEODATAFRAMES
# prepare union layer
m['micro_id'] = m.index
m.columns = ['ZONENUMMER_ANT', 'STGB_ANT', 'VREGIO_L_ANT', 'MATRIX_ANT', 'ZONENUMMER_KEM', 'STGB_KEM',
       'VREGIO_L_KEM', 'MATRIX_KEM', 'ZONENUMMER_MEC', 'STGB_MEC', 'VREGIO_L_MEC',
       'MATRIX_MEC', 'ZONENUMMER_WAA', 'STGB_WAA', 'VREGIO_L_WAA', 'MATRIX_WAA',
       'geometry', 'micro_id']
# cast all zonenummers to float
m = m.astype({'ZONENUMMER_ANT': 'float64', 'ZONENUMMER_KEM': 'float64', 'ZONENUMMER_MEC': 'float64', 'ZONENUMMER_WAA': 'float64'})
m.dropna(subset=['ZONENUMMER_ANT', 'ZONENUMMER_KEM', 'ZONENUMMER_MEC', 'ZONENUMMER_WAA'], inplace=True)
m.to_file("microregions_output.geojson", driver='GeoJSON')
# remove all columns except zonenummers and geometry - now m is a layer with microregions that is used for translation between zones id
m = m[['ZONENUMMER_ANT','ZONENUMMER_KEM', 'ZONENUMMER_MEC', 'ZONENUMMER_WAA', 'geometry', 'micro_id']]

# count duplicates in m
m['ant_count'] = m.groupby('ZONENUMMER_ANT')['ZONENUMMER_ANT'].transform('count')
m['kem_count'] = m.groupby('ZONENUMMER_KEM')['ZONENUMMER_KEM'].transform('count')
m['mec_count'] = m.groupby('ZONENUMMER_MEC')['ZONENUMMER_MEC'].transform('count')
m['waa_count'] = m.groupby('ZONENUMMER_WAA')['ZONENUMMER_WAA'].transform('count')
# get min
m['count'] = m[['ant_count', 'kem_count', 'mec_count', 'waa_count']].min(axis=1)
m['area'] = m.area
m.to_file("microregions_output.geojson", driver='GeoJSON')

### STEP 2 INDENTIFY MICROREGIONS IN EACH MATRIX OF FLOWS, CLASSIFY FLOWS MICRO-MICRO & OTHER. Do not duplicate flows.
def process_df(m, df_flows, zone_col='ZONENUMMER_ANT', zone_count_col='ant_count'):
    m2 = m[[zone_col, zone_count_col]].drop_duplicates().astype({zone_col: 'int64'})

    df_flows = df_flows.merge(m2, left_on='i', right_on=zone_col, how='left').rename(columns={zone_count_col: 'i_count'}).drop(columns=[zone_col])
    df_flows = df_flows.merge(m2, left_on='j', right_on=zone_col, how='left').rename(columns={zone_count_col: 'j_count'}).drop(columns=[zone_col])

    df_flows['flow_type'] = 'other'
    df_flows.loc[(df_flows['i_count'] == 1) & (df_flows['j_count'] == 1), 'flow_type'] = 'micro_micro'
    output_df_flows = df_flows[df_flows['flow_type'] == 'micro_micro'].copy()
    other_flows = df_flows[df_flows['flow_type'] == 'other'].copy()
    return df_flows, output_df_flows, other_flows

# get micro-micro flows
df_ANT, ant, ant_other = process_df(m, df_ANT)
#calculate % of total flows covered by micro flows
total_flows_mean = (df_ANT['[7]-Totaal'].sum() + df_KEM['[7]-Totaal'].sum() + df_MEC['[7]-Totaal'].sum() + df_WAA['[7]-Totaal'].sum())/4
print("flows in ant between microregions covers: " + str((ant['[7]-Totaal'].sum()/total_flows_mean).round(2)) + " of total flows")

df_KEM, kem, kem_other = process_df(m, df_KEM, zone_col='ZONENUMMER_KEM', zone_count_col='kem_count')
print("flows in kem between microregions covers: " + str((kem['[7]-Totaal'].sum()/total_flows_mean).round(2)) + " of total flows")
df_MEC, mec, mec_other = process_df(m, df_MEC, zone_col='ZONENUMMER_MEC', zone_count_col='mec_count')
print("flows in mec between microregions covers: " + str((mec['[7]-Totaal'].sum()/total_flows_mean).round(2)) + " of total flows")
df_WAA, waa, waa_other = process_df(m, df_WAA, zone_col='ZONENUMMER_WAA', zone_count_col='waa_count')
print("flows in waa between microregions covers: " + str((waa['[7]-Totaal'].sum()/total_flows_mean).round(2)) + " of total flows")

## drop all micro-micro flows from m
# rename i and j columns from ant id to kem id
def drop_flows_from_previous_df(df1, m, df2, zone_num_primary='ZONENUMMER_ANT', zone_num_secondary='ZONENUMMER_KEM'):
    mm = df1[['i', 'j']].copy()
    mm_del = mm.merge(m[[zone_num_primary, zone_num_secondary]], left_on='i', right_on=zone_num_primary, how='left')
    mm_del['i'] = mm_del[zone_num_secondary]
    mm_del.drop(columns=[zone_num_primary, zone_num_secondary], inplace=True)
    mm_del = mm_del.merge(m[[zone_num_primary, zone_num_secondary]], left_on='j', right_on=zone_num_primary, how='left')
    mm_del['j'] = mm_del[zone_num_secondary]
    mm_del.drop(columns=[zone_num_primary, zone_num_secondary], inplace=True)
    # drop all flows from kem between i and j, that are present in mm_del
    df2 = df2.merge(mm_del, on=['i', 'j'], how='left', indicator=True)
    df2 = df2[df2['_merge'] == 'left_only']
    df2 = df2.loc[df2.flow_type == 'micro_micro']
    df2.drop(columns=['_merge'], inplace=True)
    return df2

kem = drop_flows_from_previous_df(ant, m, df_KEM, zone_num_primary='ZONENUMMER_ANT', zone_num_secondary='ZONENUMMER_KEM')
micro_flows_covered = (ant['[7]-Totaal'].sum()+kem['[7]-Totaal'].sum())
print('ant & kem: ' + str((micro_flows_covered.round(2)/total_flows_mean).round(2)) + ' of total flows')

mec = drop_flows_from_previous_df(ant, m, df_MEC, zone_num_primary='ZONENUMMER_ANT', zone_num_secondary='ZONENUMMER_MEC')
mec = drop_flows_from_previous_df(kem, m, mec, zone_num_primary='ZONENUMMER_KEM', zone_num_secondary='ZONENUMMER_MEC')
micro_flows_covered = ant['[7]-Totaal'].sum()+kem['[7]-Totaal'].sum()+mec['[7]-Totaal'].sum()
print('ant, kem and mec: ' + str((micro_flows_covered/total_flows_mean).round(2)) + ' of total flows')

waa = drop_flows_from_previous_df(ant, m, df_WAA, zone_num_primary='ZONENUMMER_ANT', zone_num_secondary='ZONENUMMER_WAA')
waa = drop_flows_from_previous_df(kem, m, waa, zone_num_primary='ZONENUMMER_KEM', zone_num_secondary='ZONENUMMER_WAA')
waa = drop_flows_from_previous_df(mec, m, waa, zone_num_primary='ZONENUMMER_MEC', zone_num_secondary='ZONENUMMER_WAA')
micro_flows_covered = ant['[7]-Totaal'].sum()+kem['[7]-Totaal'].sum()+mec['[7]-Totaal'].sum()+waa['[7]-Totaal'].sum()
print('ant, kem, mec and waa: ' + str((micro_flows_covered/total_flows_mean).round(2)) + ' of total flows')
print('ant, kem, mec and waa: ' + str((micro_flows_covered/df_ANT['[7]-Totaal'].sum()).round(2)) + ' of total flows in antwerp matrix')

def merge_micro_ids(df, m, zone_num, micro_id):
    df = df.merge(m[[zone_num, micro_id]], left_on='i', right_on=zone_num, how='left').rename(columns={micro_id: 'i_micro_id'}).drop(columns=[zone_num])
    df = df.merge(m[[zone_num, micro_id]], left_on='j', right_on=zone_num, how='left').rename(columns={micro_id: 'j_micro_id'}).drop(columns=[zone_num])
    return df
ant = merge_micro_ids(ant, m, 'ZONENUMMER_ANT', 'micro_id')
kem = merge_micro_ids(kem, m, 'ZONENUMMER_KEM', 'micro_id')
mec = merge_micro_ids(mec, m, 'ZONENUMMER_MEC', 'micro_id')
waa = merge_micro_ids(waa, m, 'ZONENUMMER_WAA', 'micro_id')

micro_micro = pd.concat([ant, kem, mec, waa], axis=0)

micro_micro.drop(columns=['i_count', 'j_count', 'flow_type'], inplace=True)
micro_micro.to_csv("micro_micro.csv", index=False)

# STEP 3 - generate all other flows dataframes for data analysis
# save the other flows, that we missed with micro-micro relations
# add micro_ids to other flows
ant_other = merge_micro_ids(ant_other, m, 'ZONENUMMER_ANT', 'micro_id').copy()
kem_other = merge_micro_ids(kem_other, m, 'ZONENUMMER_KEM', 'micro_id').copy()
mec_other = merge_micro_ids(mec_other, m, 'ZONENUMMER_MEC', 'micro_id').copy()
waa_other = merge_micro_ids(waa_other, m, 'ZONENUMMER_WAA', 'micro_id').copy()

# calculate count of duplicated flows between i and j for every other flow df
micro_micro['micro_flow'] = micro_micro['i_micro_id'].astype(str) + "-" + micro_micro['j_micro_id'].astype(str)
micro_micro['total'] = micro_micro['[7]-Totaal']
micro_micro.to_csv("output_df.csv", index=False)

dataframes = [ant_other, kem_other, mec_other, waa_other]
other = []
for df in [ant_other, kem_other, mec_other, waa_other]:
    df['count'] = df.groupby(['i', 'j'])['i'].transform('count')
    df['micro_flow'] = df['i_micro_id'].astype(str) + "-" + df['j_micro_id'].astype(str)
    df['flow_in_micro_micro'] = df['micro_flow'].isin(micro_micro['micro_flow'])
    df = df.merge(micro_micro[['micro_flow', 'total']], on='micro_flow', how='left')
    df.rename(columns={'total': 'micro_micro_total'}, inplace=True)
    other.append(df)

other_df = pd.concat(other, axis=0)
del other

# LAST STEP - generate statistics for this analysis
# calculate differences between micro_micro flows and other flows
differences = other_df.groupby(['i','j', 'source', 'count', '[1]-Bestuurder', '[2]-Passagier', '[3]-Trein', '[4]-BTM',
                                '[5]-Fiets', '[6]-Te Voet', '[7]-Totaal'])['micro_micro_total'].sum().reset_index()
differences['diff'] = differences['[7]-Totaal'] - differences['micro_micro_total']
differences.sort_values(by=['source', 'diff'], ascending=False, inplace=True)
#add xy coordinates to differences

differences = differences.astype({'i': 'int64', 'j': 'int64'})
gdfs = gdfs.astype({'ZONENUMMER': 'int64'})
differences.sort_values(by=['source', 'i'], ascending=True, inplace=True)
differences = differences.merge(gdfs[['ZONENUMMER', 'MATRIX', 'x', 'y']],
                                left_on=['i', 'source'], right_on=['ZONENUMMER', 'MATRIX'], how='left')

differences_stats = differences[['source', '[7]-Totaal', 'micro_micro_total', 'diff']].groupby('source').describe()

# flows are missed because they are not present in micro_micro flows, but they are present in other flows
missed_flows = differences.loc[differences['micro_micro_total'] == 0]
# average percent of missed flows
print('missed flows statistics (no micro-micro flows are present, but there are other types of flows):')
for i in missed_flows['source'].unique():
    missed_flows_in_prc = (100*missed_flows.loc[missed_flows['source'] == i]['[7]-Totaal'].sum()/total_flows_mean).round(2)
    print(i + ": " + str(missed_flows_in_prc) + "% of total flows")

more_micro_than_total = differences.loc[differences['diff'] >0]
less_micro_than_total = differences.loc[differences['diff'] <0]

missed_flows_stats = missed_flows[['source', '[7]-Totaal', 'micro_micro_total', 'diff']].groupby('source').describe()
more_micro_than_total_stats = more_micro_than_total[['source', '[7]-Totaal', 'micro_micro_total', 'diff']].groupby('source').describe()
less_micro_than_total_stats = less_micro_than_total[['source', '[7]-Totaal', 'micro_micro_total', 'diff']].groupby('source').describe()

outliers1 = differences[differences["diff"] > 1000]
outliers2 = differences[differences["diff"] <= -1000]

# save micro-micro flows in geojson with geometry for i and j for kepler.gl visualization
micro_micro_gdf = m[['micro_id', 'geometry']].merge(micro_micro, left_on='micro_id', right_on='i_micro_id', how='left')
micro_micro_gdf.to_crs(epsg=4326, inplace=True)
micro_micro_gdf['xi'] = micro_micro_gdf.geometry.centroid.x
micro_micro_gdf['yi'] = micro_micro_gdf.geometry.centroid.y
micro_micro_gdf.drop(columns=['micro_id', 'geometry'], inplace=True)
micro_micro_gdf = m[['micro_id', 'geometry']].merge(micro_micro_gdf, left_on='micro_id', right_on='j_micro_id', how='left')
micro_micro_gdf.to_crs(epsg=4326, inplace=True)
micro_micro_gdf['xj'] = micro_micro_gdf.geometry.centroid.x
micro_micro_gdf['yj'] = micro_micro_gdf.geometry.centroid.y
micro_micro_gdf.drop(columns=['micro_id', 'geometry', 'Unnamed: 0'], inplace=True)
n = micro_micro_gdf[micro_micro_gdf.isna()]
micro_micro_gdf.dropna(inplace=True)
#convert i j i_micro_id and j_micro_id to int
micro_micro_gdf = micro_micro_gdf.astype({'i': 'int64', 'j': 'int64', 'i_micro_id': 'int64', 'j_micro_id': 'int64'})
micro_micro_gdf = pd.DataFrame(micro_micro_gdf)

#save all
micro_micro_gdf.to_csv("micro_micro_with_xy.csv", index=False)
differences.to_csv("differences.csv", index=False)
missed_flows.to_csv("missed_flows.csv", index=False)
more_micro_than_total.to_csv("more_micro_than_total.csv", index=False)
less_micro_than_total.to_csv("less_micro_than_total.csv", index=False)
outliers1.to_csv("outliers_diff_more_than_1K.csv", index=False)
outliers2.to_csv("outliers_diff_less_than_-1K.csv", index=False)
# all other flows are not saved because the files are larger than 300 MB - rows were duplicated many times because of of a merge macro zones & micro zones
# ant_other.to_csv(r"other\ant_other.csv", index=False)
# kem_other.to_csv(r"other\kem_other.csv", index=False)
# mec_other.to_csv(r"other\mec_other.csv", index=False)
# waa_other.to_csv(r"other\waa_other.csv", index=False)




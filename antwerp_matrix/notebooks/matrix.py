import pandas as pd
import geopandas as gpd
import os

os.chdir(r"E:\swiat_gis\freelancing projects\antwerp\antwerp_matrix\data")

# read in data
gdf_ANT = gpd.read_file("ANT_clipped_TAZ.geojson")
gdf_KEM = gpd.read_file("KEM_clipped_TAZ.geojson")
gdf_MEC = gpd.read_file("MEC_clipped_TAZ.geojson")
gdf_WAA = gpd.read_file("WAA_clipped_TAZ.geojson")

df_ANT = pd.read_csv("ANT_clipped_MATRIX.csv")
df_ANT['source'] = 'ANT'
df_KEM = pd.read_csv("KEM_clipped_MATRIX.csv")
df_KEM['source'] = 'KEM'
df_MEC = pd.read_csv("MEC_clipped_MATRIX.csv")
df_MEC['source'] = 'MEC'
df_WAA = pd.read_csv("WAA_clipped_MATRIX.csv")
df_WAA['source'] = 'WAA'

m = gpd.read_file("union.geojson")

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
#todo solve it later
# x = m[m['count']>1]
# x.to_file("wtf_microregions.geojson", driver='GeoJSON')
# todo handle later [1732, 1267, ] & verry small areas: 1337, 2133, 5

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
micro_flows_covered = (ant['[7]-Totaal'].sum()+kem['[7]-Totaal'].sum())/total_flows_mean
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


# save the other flows, that we missed with micro-micro relations
# add micro_ids to other flows
ant_other = merge_micro_ids(ant_other, m, 'ZONENUMMER_ANT', 'micro_id')
kem_other = merge_micro_ids(kem_other, m, 'ZONENUMMER_KEM', 'micro_id')
mec_other = merge_micro_ids(mec_other, m, 'ZONENUMMER_MEC', 'micro_id')
waa_other = merge_micro_ids(waa_other, m, 'ZONENUMMER_WAA', 'micro_id')

# calculate count of duplicated flows between i and j for every other flow df
micro_micro['micro_flow'] = micro_micro['i_micro_id'].astype(str) + "-" + micro_micro['j_micro_id'].astype(str)
micro_micro['total'] = micro_micro['[7]-Totaal']
dataframes = [ant_other, kem_other, mec_other, waa_other]
for df in [ant_other, kem_other, mec_other, waa_other]:
    df['count'] = df.groupby(['i', 'j'])['i'].transform('count')
    df['micro_flow'] = df['i_micro_id'].astype(str) + "-" + df['j_micro_id'].astype(str)
    df['flow_in_micro_micro'] = df['micro_flow'].isin(micro_micro['micro_flow'])
    # divde all flow columns by count
    for col in df.columns:
        if '[7]-Totaal' in col:
            df[col] = df[col]/df['count']

ant_other = ant_other.merge(micro_micro[['micro_flow', 'total']], on='micro_flow', how='left')
kem_other = kem_other.merge(micro_micro[['micro_flow', 'total']], on='micro_flow', how='left')
mec_other = mec_other.merge(micro_micro[['micro_flow', 'total']], on='micro_flow', how='left')
waa_other = waa_other.merge(micro_micro[['micro_flow', 'total']], on='micro_flow', how='left')

# save all dataframes in output folder
micro_micro.to_csv("output_df.csv", index=False)
# large files because other flows were duplicated for every zone that covered more then 1 microregion
# ant_other.to_csv(r"other\ant_other.csv", index=False)
# kem_other.to_csv(r"other\kem_other.csv", index=False)
# mec_other.to_csv(r"other\mec_other.csv", index=False)
# waa_other.to_csv(r"other\waa_other.csv", index=False)



# an example of other flows problem
x = waa_other[(waa_other['i'] == 320) & (waa_other['j'] == 1290)]
x.total.sum()
x['[7]-Totaal'].sum()

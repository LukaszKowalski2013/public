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

# prepare union layer
m['micro_id'] = m.index
m.columns = ['ZONENUMMER_ANT', 'STGB_ANT', 'VREGIO_L_ANT', 'MATRIX_ANT', 'ZONENUMMER_KEM', 'STGB_KEM',
       'VREGIO_L_KEM', 'MATRIX_KEM', 'ZONENUMMER_MEC', 'STGB_MEC', 'VREGIO_L_MEC',
       'MATRIX_MEC', 'ZONENUMMER_WAA', 'STGB_WAA', 'VREGIO_L_WAA', 'MATRIX_WAA',
       'geometry', 'micro_id']
# cast all zonenummers to float
m = m.astype({'ZONENUMMER_ANT': 'float64', 'ZONENUMMER_KEM': 'float64', 'ZONENUMMER_MEC': 'float64', 'ZONENUMMER_WAA': 'float64'})
m.to_file("microregions_output.geojson", driver='GeoJSON')
m = m[['ZONENUMMER_ANT','ZONENUMMER_KEM', 'ZONENUMMER_MEC', 'ZONENUMMER_WAA', 'geometry', 'micro_id']]

# merge m with dfs on i
def process_df(df, m, zonenummer, merge_on):
    df = pd.merge(df, m[[zonenummer, 'micro_id']], left_on=merge_on, right_on=zonenummer, how='left')
    print(df.micro_id.isna().sum())
    df.rename(columns={'micro_id': f'{merge_on}_micro_id'}, inplace=True)
    df.drop(columns=[zonenummer], inplace=True)
    return df

df_list = [df_ANT, df_KEM, df_MEC, df_WAA]
zonenummer_list = ['ZONENUMMER_ANT', 'ZONENUMMER_KEM', 'ZONENUMMER_MEC', 'ZONENUMMER_WAA']
new_df_list = []
for df, zonenummer in zip(df_list, zonenummer_list):
    df = process_df(df, m, zonenummer, 'i')
    df = process_df(df, m, zonenummer, 'j')
    new_df_list.append(df)

# concat all dfs
df = pd.concat(new_df_list)
print(df['i_micro_id'].isna().sum())
print(df['j_micro_id'].isna().sum())

# save to csv
df.to_csv("matrix.csv", index=False)
m.to_file("microregions_ids.geojson", driver='GeoJSON')

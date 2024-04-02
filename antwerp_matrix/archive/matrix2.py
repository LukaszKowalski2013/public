import pandas as pd
import geopandas as gpd
import os

os.chdir(r"E:\swiat_gis\freelancing projects\antwerp\antwerp_matrix\data")


def get_microregions():
    # load all geojsons
    gdf_ANT = gpd.read_file("ANT_clipped_TAZ.geojson")
    gdf_KEM = gpd.read_file("KEM_clipped_TAZ.geojson")
    gdf_MEC = gpd.read_file("MEC_clipped_TAZ.geojson")
    gdf_WAA = gpd.read_file("WAA_clipped_TAZ.geojson")
    all_gdfs = pd.concat([gdf_ANT, gdf_KEM, gdf_MEC, gdf_WAA])
    # sort all gdfs by area
    all_gdfs['AREA'] = all_gdfs.area
    all_gdfs = all_gdfs.sort_values(by='AREA', ascending=False)

    # cols: ['ZONENUMMER', 'STGB', 'VREGIO_L', 'MATRIX', 'geometry'], dtype='object')
    # add suffix to all columns in all geojsons
    for gdf in [gdf_ANT, gdf_KEM, gdf_MEC, gdf_WAA]:
        # calculate area
        gdf.geometry = gdf['geometry']
        gdf['AREA'] = gdf.area
        suffix = gdf['MATRIX'].iloc[0]
        gdf.columns = [f"{col}_{suffix}" for col in gdf.columns]

    # calulate geometrical union of all gdfs
    gdf_ANT = gdf_ANT.set_geometry('geometry_ANT')
    gdf_WAA = gdf_WAA.set_geometry('geometry_WAA')
    microregions = gpd.overlay(gdf_ANT, gdf_WAA, how='union', keep_geom_type=True)
    # drop all microregions that have area smaller than min_area
    microregions['UNION_AREA'] = microregions.area

    gdf_KEM = gdf_KEM.set_geometry('geometry_KEM')
    microregions = gpd.overlay(microregions, gdf_KEM, how='union', keep_geom_type=True)
    microregions['UNION_AREA'] = microregions.area

    gdf_MEC = gdf_MEC.set_geometry('geometry_MEC')
    microregions = gpd.overlay(microregions, gdf_MEC, how='union', keep_geom_type=True)
    microregions['UNION_AREA'] = microregions.area

    return microregions, all_gdfs


microregions, all_gdfs = get_microregions()


def fix_geometries(microregions):
    # small areas
    smallest_microregions = len(microregions['UNION_AREA'] < 0.1)
    microregions.loc[microregions['UNION_AREA'] < 0.1].to_file("smallest_microregions.geojson", driver='GeoJSON')
    print("deleting microregions that have area equal to 0.1 m2 (in total:" + str(smallest_microregions) + ")")
    microregions = microregions.loc[microregions['UNION_AREA'] >= 0.1]

    # multipolygons
    microregions['geometry_type'] = microregions.geom_type
    multipolygons = microregions.loc[microregions['geometry_type'] == 'MultiPolygon']

    multipolygons['area'] = multipolygons['geometry'].area

    ifMultipolygons = microregions['geometry_type'] == 'MultiPolygon'
    ifAreaSmall = microregions['UNION_AREA'] < 323118
    multipolygons = microregions.loc[ifMultipolygons & ifAreaSmall]
    multipolygons.to_file("multipolygons_deleted.geojson", driver='GeoJSON')
    multipolygons_number = len(multipolygons)
    print("Some of the regions are multipolygons. Fix them manually. In total: " + str(multipolygons_number)
          + ". Smallest multipolygons were deleted (smaller then 323118 m2) "
            "and saved to file multipolygons_deleted.geojson")
    multipolygons_to_save = microregions.loc[ifMultipolygons & ~ifAreaSmall]
    microregions = microregions.loc[~ifMultipolygons]
    microregions = gpd.GeoDataFrame(pd.concat([microregions, multipolygons_to_save], axis=0))

    return microregions.copy()


microregions = fix_geometries(microregions)


# count duplicates of ids for every region
def count_duplicates(microregions):
    microregions['AND_count'] = microregions.groupby('ZONENUMMER_ANT')['ZONENUMMER_ANT'].transform('count')
    microregions['KEM_count'] = microregions.groupby('ZONENUMMER_KEM')['ZONENUMMER_KEM'].transform('count')
    microregions['MEC_count'] = microregions.groupby('ZONENUMMER_MEC')['ZONENUMMER_MEC'].transform('count')
    microregions['WAA_count'] = microregions.groupby('ZONENUMMER_WAA')['ZONENUMMER_WAA'].transform('count')
    return microregions


microregions = count_duplicates(microregions)
microregions['geometry_type'].unique()


def get_microregions_id(microregions):
    # add column to res_union with the name of the region that has the smallest area
    microregions['MIN_AREA'] = microregions[['AREA_ANT', 'AREA_KEM', 'AREA_MEC', 'AREA_WAA']].min(axis=1)
    microregions['MIN_AREA_REGION'] = microregions[['AREA_ANT', 'AREA_KEM', 'AREA_MEC', 'AREA_WAA']].idxmin(axis=1)
    microregions['UNION_AREA'] = microregions.area
    microregions['x'] = microregions.centroid.x
    microregions['y'] = microregions.centroid.y
    microregions['MICROREGION'] = microregions['MIN_AREA_REGION'].apply(lambda x: x.split('_')[1])
    microregions['ID_MICROREGION'] = microregions.reset_index().index
    return microregions


microregions = get_microregions_id(microregions)


def get_translation_df(microregions):
    ant = microregions[['ZONENUMMER_ANT', 'ID_MICROREGION']].copy()
    ant['ZONE'] = 'ANT'
    ant.dropna(inplace=True)
    ant.drop_duplicates(inplace=True)
    ant.columns = ['ZONENUMMER', 'ID_MICROREGION', 'ZONE']
    ant['ZONENUMMER'] = ant['ZONENUMMER'].astype(int)
    kem = microregions[['ZONENUMMER_KEM', 'ID_MICROREGION']].copy()
    kem['ZONE'] = 'KEM'
    kem.dropna(inplace=True)
    kem.drop_duplicates(inplace=True)
    kem.columns = ['ZONENUMMER', 'ID_MICROREGION', 'ZONE']
    kem['ZONENUMMER'] = kem['ZONENUMMER'].astype(int)
    mec = microregions[['ZONENUMMER_MEC', 'ID_MICROREGION']].copy()
    mec['ZONE'] = 'MEC'
    mec.dropna(inplace=True)
    mec.drop_duplicates(inplace=True)
    mec.columns = ['ZONENUMMER', 'ID_MICROREGION', 'ZONE']
    mec['ZONENUMMER'] = mec['ZONENUMMER'].astype(int)
    waa = microregions[['ZONENUMMER_WAA', 'ID_MICROREGION']].copy()
    waa['ZONE'] = 'WAA'
    waa.dropna(inplace=True)
    waa.drop_duplicates(inplace=True)
    waa.columns = ['ZONENUMMER', 'ID_MICROREGION', 'ZONE']
    waa['ZONENUMMER'] = waa['ZONENUMMER'].astype(int)
    all = pd.concat([ant, kem, mec, waa], axis=0)
    # count how many microregions are in each ZONENUMMER & ZONE and add it as field
    all['COUNT'] = all.groupby(['ZONENUMMER', 'ZONE'])['ID_MICROREGION'].transform('count')
    all.sort_values(by=['COUNT'], inplace=True)
    return all, ant, kem, mec, waa


# now we have a dataframe with all microregions id mapped to all regions id based on microregions geodf
all, ant, kem, mec, waa = get_translation_df(microregions)
all.to_csv("all.csv", index=False)


def add_microregion_id(ant, kem, mec, waa):
    df_ANT = pd.read_csv("ANT_clipped_MATRIX.csv")
    df_ANT['source'] = 'ANT'
    df_KEM = pd.read_csv("KEM_clipped_MATRIX.csv")
    df_KEM['source'] = 'KEM'
    df_MEC = pd.read_csv("MEC_clipped_MATRIX.csv")
    df_MEC['source'] = 'MEC'
    df_WAA = pd.read_csv("WAA_clipped_MATRIX.csv")
    df_WAA['source'] = 'WAA'

    all_df = []
    for df, micro_df in [[df_ANT, ant], [df_KEM, kem], [df_MEC, mec], [df_WAA, waa]]:
        df = df.merge(micro_df, left_on='i', right_on='ZONENUMMER', how='left')
        df.rename(columns={'ID_MICROREGION': 'i_micro'}, inplace=True)
        df.drop(columns=['ZONENUMMER', 'ZONE'], inplace=True)
        df = df.merge(micro_df, left_on='j', right_on='ZONENUMMER', how='left')
        df.rename(columns={'ID_MICROREGION': 'j_micro'}, inplace=True)
        df.drop(columns=['ZONENUMMER', 'ZONE'], inplace=True)
        all_df.append(df)
    output = pd.concat(all_df, axis=0)
    output = output[['i', 'j', 'i_micro', 'j_micro', 'source']]
    return output


output = add_microregion_id(ant, kem, mec, waa)


# add flows data to output
def add_flows_data(output):
    df_ANT = pd.read_csv("ANT_clipped_MATRIX.csv")
    df_KEM = pd.read_csv("KEM_clipped_MATRIX.csv")
    df_MEC = pd.read_csv("MEC_clipped_MATRIX.csv")
    df_WAA = pd.read_csv("WAA_clipped_MATRIX.csv")
    all_df = []
    for df, source in [[df_ANT, 'ANT'], [df_KEM, 'KEM'], [df_MEC, 'MEC'], [df_WAA, 'WAA']]:
        df['source'] = source
        all_df.append(df)
    all = pd.concat(all_df, axis=0)
    all.drop('Unnamed: 0', axis=1, inplace=True)
    flows = output.merge(all, on=['i', 'j', 'source'], how='left')
    # count duplicated flows
    flows['COUNT'] = flows.groupby(['i', 'j', 'source'])['[7]-Totaal'].transform('count')
    # check: do we have good number of total flows?
    flows['total_calibrated'] = flows['[7]-Totaal'] / flows['COUNT']
    if (flows['total_calibrated'].sum() - all['[7]-Totaal'].sum() > 1):
        print("There is a problem with the total number of flows. Check the data.")
    flows.drop('total_calibrated', axis=1, inplace=True)

    return flows


output_flows = add_flows_data(output)


def add_xy_to_flows(output_flows, microregions):
    output_flows = output_flows.merge(microregions[['ID_MICROREGION', 'x', 'y']], left_on='i_micro',
                                      right_on='ID_MICROREGION', how='left')
    output_flows.rename(columns={'x': 'x_i', 'y': 'y_i'}, inplace=True)
    output_flows.drop('ID_MICROREGION', axis=1, inplace=True)
    output_flows = output_flows.merge(microregions[['ID_MICROREGION', 'x', 'y']], left_on='j_micro',
                                      right_on='ID_MICROREGION', how='left')
    output_flows.rename(columns={'x': 'x_j', 'y': 'y_j'}, inplace=True)
    output_flows.drop('ID_MICROREGION', axis=1, inplace=True)
    return output_flows


output_flows = add_xy_to_flows(output_flows, microregions)

output.to_csv("output_light.csv", index=False)
microregions.to_file("microregions.geojson", driver='GeoJSON')
output_flows.to_csv("output_flows.csv", index=False)

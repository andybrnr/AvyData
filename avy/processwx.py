"""
@author: ABerner
"""
from math import sin, cos, sqrt, atan2, radians
from os import listdir

import pandas as pd
from fetchwx import mwnet_dict

#implementation courtesy of Stack Overflow
#http://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
#approx, as doesn't include WGS84 corrections
def calc_dist(pt1, pt2):
    '''
    Calculates great-circle distance between two points on a sphere, which is
    handy for selecting stations within some range of a desired point
    
    Parameters:
    -----------
    pt1 (tuple) coordinates of first point in (lat, lon) decimal form 
                [units: degrees]
    pt2 (tuple) coordinates of second point in (lat, lon) decimal form
                [units: degrees]
    
    Returns:
    --------
    dist (float) great-circle distance between pt1 and pt2 for spheroidal earth
                 approximation [units: km]
    '''
    R = 6373.0
    
    lat1 = radians(pt1[0])
    lon1 = radians(pt1[1])
    lat2 = radians(pt2[0])
    lon2 = radians(pt2[1])
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c
    

def select_stn(datadir, args, return_df=False):
    '''
    Provides a list of downloaded weather stations meeting criteria in args.
    
    Parameters:
    -----------
    datadir (str) path to directory where station data is stored
    args (dict) dictionary used to filter station selections, where keys 
                match column names in metadata file
    
    Returns:
    --------
    stn_list (list) list of station id strings corresponding to station data 
                    files
    '''
    file_list = listdir(datadir)
    md_file = [file for file in file_list if 'metadata' in file.lower()]
    if len(md_file) != 1:
        print("more than one metadatafile, check directory setup")
        return None
    filepath = datadir + '/' + md_file[0]
    df = pd.read_csv(filepath, compression='gzip', index_col=[0])
    df.columns = [col.lower() for col in df.columns]
    
    args_keys = args.keys()
    allowed_keys = ['mnet', 'elevation', 'state', 'max_dist', 'k_nrst',
                    'lat_lon']
    if not set(args_keys).issubset(allowed_keys):
        print("bad keys in args")
        return None
    
    if ('max_dist' in args_keys) or ('k_nrst' in args_keys):
        if 'lat_lon' not in args_keys:
            print("must provide coordinates for proximity calculations")
            return None
        else:
            pt1 = args['lat_lon']
            df['dist'] = df.apply(lambda x: calc_dist(pt1,(x['latitude'],
                                  x['longitude'])), axis=1)
    if 'mnet' in args_keys:
        try:
            nets = [mwnet_dict[mnet] for mnet in args['mnet']]
        except KeyError:
            print("mesonet string not recognized")
            return None
        df = df[df['mnet_id'].isin(nets)]
    if 'state' in args_keys:
        states = [state.upper() for state in args['state']]
        df = df[df['state'].isin(states)]
    if 'elevation' in args_keys:
        el_min = args['elevation']['min']
        el_max = args['elevation']['max']
        df = df[(df['elevation'] < el_max) & (df['elevation'] > el_min)]
    if 'k_nrst' in args_keys and 'max_dist' in args_keys:
        print("can only specify one of k_nrst or max_dist")
        return None
    if 'k_nrst' in args_keys:
        k = args['k_nrst']
        df = df.sort_values(by='dist').reset_index(drop=True)
        df = df.iloc[0:min(k,len(df))]
    if 'max_dist' in args_keys:
        dmax = args['max_dist']
        df = df[df['dist'] <= dmax]
    if df.empty:
        print("No stations meet criteria")
        return None
    else:
        if return_df:
            return df
        else:
            return list(df['stid'].values)

            
def process_stn(datadir, stnid, clean=True):
    filepath = '{0}/{1}.csv'.format(datadir, stnid)
    skiprows = [0,1,2,3,4,5,7]
    df = pd.read_csv(filepath, skiprows=skiprows, index_col=[1], 
                     parse_dates=[1], compression='gzip')
    if 'heat_index_set_1d' in df.columns:
        df = df.drop('heat_index_set_1d', axis=1)
    df = df.convert_objects(convert_numeric=True)
    return df


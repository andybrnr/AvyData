"""
@author: ABerner
"""
from os import listdir
from os.path import isfile
import re
from gzip import GzipFile
import json

import pandas as pd
import requests
import yaml

mwnet_dict = {'SNOTEL': 25,
              'NWAC': 37,
              'BTAVAL': 48}

with open('synoptic_config.yml', 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

api_token = cfg['SynopticAPI']['token']

today = pd.datetime.today()
today = (today.year, today.month, today.day, today.hour, today.minute)


class MwFetcher(object):
    '''
    Class wrapping basic functionality of Synoptic API for MesoWest stations.
    Implements metadata and timeseries requests.
    '''
    def __init__(self, token=api_token):
        self.api_url = 'https://api.mesowest.net/v2/'
        self.api_token = api_token
    
    def fetch_networks(self):
        '''
        Retrieve data on the available station networks.
        '''
        url = self.api_url + 'networks?&token=' + self.api_token
        response = requests.get(url)
        return json.loads(response.content)
    
    def fetch_stn_metadata(self, networks, args={'state':('WA',),
                           'status':('active',)}):
        '''
        Retrieve metadata for stations in specified mesonets 
        
        Gets station metadata for stations in mesonets with MNET_IDs specified 
        in networks. Provide a dict of args for extra api arguments; all values
        must be tuples.
        '''
        url = (self.api_url + 'stations/metadata?' +
              '&network=' + ','.join([str(net_id) for net_id in networks]))
        for key, value in args.items():
            url = (url + '&' + key + '=' + 
                   ','.join([str(val) for val in value]))
        url = url + '&token=' + self.api_token
        response = requests.get(url)
        return json.loads(response.content)

    def fetch_stn_ts(self, stids, output='JSON', start_date=(1997,1,1),
                     end_date=today):
        '''
        Retrieve station timeseries
        
        Gets station observations in date range [start_date:end_date]; can
        specify either JSON or CSV. JSON only available for periods shorter
        than two years. Return none and print error message if API throws an
        error.
        '''
        start = pd.datetime(*start_date)
        end = pd.datetime(*end_date)
        start_str = start.strftime('%Y%m%d%H%M')
        end_str = end.strftime('%Y%m%d%H%M')
        url = (self.api_url + 'stations/timeseries?&stid=' + ','.join(stids) + 
               '&start=' + start_str + '&end=' + end_str + 
               '&output=' + output + '&token=' + self.api_token)
        response = requests.get(url)
        try: 
            out = json.loads(response.content)
            if out['SUMMARY']['RESPONSE_CODE'] == -1:
                print(out['SUMMARY']['RESPONSE MESSAGE'])
                return None
        except json.JSONDecodeError:
            if output == 'JSON':
                print("Request too large for JSON response")
                return None
            else:
                pass
        return response.content


def fetch_mnet_ts(networks, args, outdir, start_date=(1997,1,1), 
                  end_date=today):
    '''
    Retrieve and archive metadata and station timeseries 
    
    Gets metadata and observation timeseries stations for the specified 
    networks (by MNET_ID) and dict of API args (e.g. {'status':('Active',), 
    'state':('WA','OR')}) in the date range. Current implementation is to save
    a metadata file and station files in CSV format in directory outdir.
    Existing CSVs are updated with latest available data.
    '''
    
    def md_json_to_df(md_json):
        df = pd.DataFrame(md_json['STATION'])
        df['REC_START'] = df['PERIOD_OF_RECORD'].apply(lambda x: x['start'])
        df['REC_START'] = pd.to_datetime(df['REC_START'])
        df['REC_END'] = df['PERIOD_OF_RECORD'].apply(lambda x: x['end'])
        df['REC_END'] = pd.to_datetime(df['REC_END'])
        df = df[['STID', 'MNET_ID', 'NAME', 'LATITUDE', 'LONGITUDE',
                     'ELEVATION', 'STATE', 'REC_START', 'REC_END']]
        return df
    
    fetcher = MwFetcher(token=api_token)
    md_df = md_json_to_df(fetcher.fetch_stn_metadata(networks,args=args))
    md_df.to_csv(outdir + 'stn_metadata_MNETIDs_' + 
                  '_'.join([str(net_id) for net_id in networks]) + '.csv',
                  compression = 'gzip')
    stids = md_df['STID'].values
    fls = [f.split('.csv')[0] for f in listdir(outdir) if (isfile(outdir+f)
               and not f.startswith('stn_metadata'))]
    p = re.compile('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)') #datetime regex
    for stid in stids: 
        tmp_strt_dt = start_date
        s = ''
        if stid in fls:
            print(stid + " exists, updating")
            f = outdir + stid + '.csv'
            with GzipFile(filename=f, mode='r') as fin:
                s = fin.read().decode(encoding='utf-8')
            lst_ln_idx = s[0:-2].rfind('\n')+1
            lst_ln = s[lst_ln_idx:]
            dt_str = p.search(lst_ln)
            if dt_str:
                dt = pd.to_datetime(dt_str.group())
                tmp_strt_dt = (dt.year,dt.month,dt.day,dt.hour,dt.minute+1)
            else:
                print("Failed to find end date in file. Skipping " + stid)
                continue
        resp = fetcher.fetch_stn_ts([stid], output='CSV', 
                                    start_date=tmp_strt_dt, end_date=end_date)
        if resp:
            if s:
                s1 = resp.decode(encoding='utf-8')
                match = p.search(s1)
                if match:
                    idx = match.start()
                    fst_ln_idx = s1.rfind('\n',0,idx)+1
                    s1 = s1[fst_ln_idx:]
                    s = ''.join([s,s1])
                    resp = s.encode(encoding='utf-8')
                else:
                    print("No data in response. Skipping update to " + stid)
                    continue
            filename = outdir + stid + '.csv'
            with GzipFile(filename=filename, mode='w') as fout:
                fout.write(resp)
            print("Wrote " + stid + " to file")
        else:
            print(stid + " failed to write")

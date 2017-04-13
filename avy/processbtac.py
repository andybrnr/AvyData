"""
@author: ABerner
"""
import pandas as pd
import re
import json
from bs4 import BeautifulSoup
from common import GzipJsonFile
from gzip import GzipFile

def process_btac_nowcast(infile, outfile, cutoff=15000):
    '''
    Takes the html files saved from the daily avalanche bulletins and extracts
    the hazard ratings at different elevations for the morning and afternoon.
    Writes cleaned output to .csv.
    
    TO-DO: bulletins for the Continental Divide and Grey's Pass regions 
    somewhat inconveniently encode the hazard only in gif form, cannot be
    scraped from text. Will need to add image download/processing to make this
    work. Eventually would also like to extract avalanche problem details.
    '''
    df = pd.DataFrame()
    
    def hzrd_mapper(text):
        text = text.lower()
        if 'low' in text:
            return 1
        elif 'moderate' in text:
            return 2
        elif 'considerable' in text:
            return 3
        elif 'high' in text:
            return 4
        elif 'extreme' in text:
            return 5
        else:
            return 0
    
    lvl_dict = {0: 'atl',
                1: 'tl',
                2: 'btl'}
                    
    df_idx = 0
    with GzipJsonFile(filename=infile, mode='r') as fin:
        for line in fin:
            if len(line['content']) < cutoff:
                continue
            
            data_row = {}
            s = BeautifulSoup(line['content'],"lxml")
            
            fcst_header = s.find_all('div', class_='forecast-headline-box')
            header_text = ''.join([elem.get_text() for elem in fcst_header])
            p = re.compile('(\d{2}/\d{2}/\d{4})')
            dt = p.search(header_text).group()
            data_row.update({'date': dt})
            
            header_text = header_text.lower()
            if 'teton' in header_text:
                region = 'teton'
            elif 'continental divide' in header_text:
                region = 'tog'
            elif 'grey' in header_text:
                region = 'grey'
            else:
                print("region not recognized, skipping...")
                continue
            data_row.update({'region': region})
            
            mtn_wx_tbl = s.find_all('table', class_='mtnWeather')
            if 'teton_print' in line['url'] and region == 'teton':
                hzrd_tbl = mtn_wx_tbl[2]
                rows = hzrd_tbl.find_all('tr')
                
                for i, row in enumerate(rows):
                    cols = row.find_all('td')
                    cols = [elem.text.strip() for elem in cols]
                    data_row.update({lvl_dict[i]+'_am': hzrd_mapper(cols[1]),
                                     lvl_dict[i]+'_pm': hzrd_mapper(cols[2])})
            else:
                print("hazard graphic parsing not yet implemented")
                continue
            df = df.append(pd.DataFrame(data_row, index=[df_idx]))
            df_idx += 1
    df['date'] = pd.to_datetime(df['date'])
    df['dt_am'] = df['date'].apply(lambda x: 
                                   pd.datetime(x.year, x.month, x.day, 9))
    df['dt_pm'] = df['date'].apply(lambda x:
                                   pd.datetime(x.year, x.month, x.day, 15))
    
    col_names = ['date', 'region', 'atl', 'tl', 'btl']
    df_am = df[['dt_am', 'region', 'atl_am', 'tl_am', 'btl_am']]
    df_am.columns = col_names
    df_am = df_am.set_index('date')
    df_pm = df[['dt_pm', 'region', 'atl_pm', 'tl_pm', 'btl_pm']]
    df_pm.columns = col_names
    df_pm = df_pm.set_index('date')
    df = pd.concat([df_am,df_pm])
    df = df.sort_index()
    df.to_csv(outfile, compression='gzip')


def process_btac_events(infile, outfile):
    '''
    Takes serialized JSON file of BTAC avalanche events and writes to .csv for 
    ease of conversion to a pandas dataframe.
    '''
    with GzipFile(filename=infile, mode='r') as fin:
        dd = json.loads(fin.read())
    
    type_mapper = {'ID': float,
                   'affiliation': str,
                   'aspect': str,
                   'avy_trigger': str,
                   'depth': float,
                   'destructive_size': float,
                   'elevation': float,
                   'event_date': str,
                   'event_time': str,
                   'event_year': float,
                   'fatality': float,
                   'fldType': str,
                   'lat': float,
                   'lng': float,
                   'notes': str,
                   'observer': str,
                   'pathname': str,
                   'relative_size': float,
                   'slope_angle': str,
                   'zone': str}
    
    df = pd.DataFrame(dd['data'])
    for col in type_mapper.keys():
        df[col] = df[col].astype(type_mapper[col])
    df['event_date'] = pd.to_datetime(df['event_date'])
    df = df[['ID', 'event_date', 'event_time',
             'zone', 'pathname', 'elevation', 'lat', 'lng', 'aspect',
             'slope_angle', 'destructive_size','relative_size', 'depth',
             'avy_trigger', 'fldType', 'fatality', 'observer', 'affiliation', 
             'notes']]
    df.to_csv(outfile, compression='gzip')

"""
@author: ABerner
"""

import pandas as pd
import requests
import json
import xmltodict
from gzip import GzipFile

from common import DataFetcher


def fetch_btac_events(outfile='btac_events.txt.gz', start_date=(2000,1,1), 
                      end_date=(today[0],today[1],today[2])):
    '''
    Fetch all the BTAC recorded avalanche events in specified date range
    
    <<more doc here>>
    '''
    base_url = ('http://www.jhavalanche.org/lib/avy_events.php?action=get'
                '&start={1:02}%2F{2:02}%2F{0}&end={4:02}%2F{5:02}%2F{3}'
                '&areas=All+areas&')
    
    url = base_url.format(*start_date,*end_date)
    response = requests.get(url)
    try: 
        dd = json.loads(response.content)
        drop_keys = [str(i) for i in range(26)]
        for i in range(len(dd['data'])):
            for k in drop_keys:
                dd['data'][i].pop(k, None)
        dd['data'].sort(key=lambda obs: pd.to_datetime(obs['event_date']))
        
        filename = outfile
        with GzipFile(filename=filename, mode='w') as fout:
                fout.write(json.dumps(dd).encode(encoding='utf-8'))
    except json.JSONDecodeError:
        print("Response did not contain valid JSON")
    
    
def fetch_btac_obs(outfile='btac_obs.txt.gz', start_date=(2000,1,1),
                   end_date=(today[0],today[1],today[2])):
    '''
    Fetch all the BTAC observations in specified date range
    
    <<more doc here>>
    '''
    
    base_url = 'http://www.jhavalanche.org/lib/obs_xml.php'
    dd = {}
    dd['data'] = []
    yrs_chunk = 5
    tmp_start, tmp_end = start_date, start_date
    tmp_end = (tmp_end[0] + yrs_chunk, tmp_end[1], tmp_end[2])
    while tmp_start < end_date:
        tmp_end = min(tmp_end, end_date)
        #hack to deal with bad data on 02/14/2013
        if tmp_start < (2013,2,14) and tmp_end > (2013,2,14):
            tmp_end = (2013,2,13)
     
        print(tmp_start, tmp_end)
        session = requests.Session()
        session.head('http://www.jhavalanche.org/observations/viewObs') #set cookies
        response = session.post(
                url=base_url,
                data={'start_date': '{1:02}/{2:02}/{0}'.format(*tmp_start),
                      'end_date': '{1:02}/{2:02}/{0}'.format(*tmp_end),
                      'area': 'All areas',
                      'zone': '0',
                      'approved': '1'},
                headers={'Referer': base_url})
        try:
            #hack to fix bad data on 02/14/2013
            s = response.content
            if tmp_start <= (2013,2,14) and tmp_end >= (2013,2,14):
                idx = s.rfind(b'/>')
                s = s[0:idx + 2] + b'</markers>\n\n'
                
            tmp_dd = xmltodict.parse(s)
            if tmp_dd['markers']:
                for item in tmp_dd['markers']['marker']:
                    item = dict(item)
                    dd['data'].append(item)
            else:
                pass
        except xmltodict.expat.ExpatError:
            print("XML Response cannot be parsed")
            print(response.content)
        
        session.close()
        tmp_start = tmp_end
        tmp_start = pd.datetime(*tmp_end) + pd.offsets.Day(1)
        tmp_start = (tmp_start.year, tmp_start.month, tmp_start.day)
        tmp_end = (tmp_end[0] + yrs_chunk,tmp_end[1], tmp_end[2])
    
    dd['data'].sort(key=lambda x: pd.to_datetime(x['@obs_date']))
    filename = outfile
    with GzipFile(filename=filename, mode='w') as fout:
        fout.write(json.dumps(dd).encode(encoding='utf-8'))
        

def fetch_btac_advisory(outfile, area='teton', start_yr=1999, 
                        end_yr=today[0], start_date=(11,1), end_date=(5,30)):
    base_url = 'http://www.jhavalanche.org/view'
    base_url_dict = {'teton': base_url + 'Teton?data_date={0}' +
                              '&template=teton_print.tpl.php',
                     'tog': base_url + 'Other?area=tog&data_date={0}',
                     'grey': base_url + 'Other?area=greay&data_date={0}'}
    urls = []
    for year in range(start_yr, end_yr):
        current_day = datetime.date(year, start_date[0], start_date[1])
        end_day = datetime.date(year+1, end_date[0], end_date[1])
        while current_day < end_day:
            urls.append(base_url_dict[area].format(current_day))
            current_day = current_day + datetime.timedelta(1)
        year += 1
    fetcher = DataFetcher()
    fetcher.fetch_pages(urls, outfile)
        
    
def fetch_btac_evening_fcst(outfile, start_yr=2005, end_yr = today[0], 
                            start_date=(11,1), end_date=(5,30)):
    base_url = 'http://www.jhavalanche.org/viewAdvisory?&data_date={0}'  
    urls = []
    for year in range(start_yr, end_yr):
        current_day = datetime.date(year, start_date[0], start_date[1])
        end_day = datetime.date(year+1, end_date[0], end_date[1])
        while current_day < end_day:
            urls.append(base_url.format(current_day))
            current_day = current_day + datetime.timedelta(1)
        year += 1
    fetcher = DataFetcher()
    fetcher.fetch_pages(urls, outfile)
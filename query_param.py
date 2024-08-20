import planetary_computer
from netCDF4 import Dataset
import fsspec
import pystac_client
import numpy as np
import os
import concurrent.futures
from urllib.parse import urlparse
from urllib.request import urlretrieve
from bitstring import Bits, pack
from datetime import date, timedelta,datetime

import sys
import OpenVisus as ov
base_date = date(1950, 1, 1)
present_date = date(2015, 1, 1)
last_date = date(2100, 12, 31)
cache_base='.azrcache'

try:
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
    )
    collection = catalog.get_collection("nasa-nex-gddp-cmip6")
    variable_list=collection.summaries.get_list("cmip6:variable")
    model_list=collection.summaries.get_list("cmip6:model")[:10]
    scenario_list=collection.summaries.get_list("cmip6:scenario")
    have_pc = True
except pystac_client.exceptions.APIError:
    print("don't have planetary computer api access")
    have_pc = False

def split_str(s):
    a,b=s.split('_')
    return int(a),int(b)

def _get_dataset(url):
    path = urlparse(url).path
    cache_entry = f'{cache_base}/{path}'
    if not os.path.exists(cache_entry):
        cache_dir = os.path.dirname(cache_entry)
        if not os.path.exists(cache_dir):
            os.makedirs(os.path.dirname(cache_entry))
        urlretrieve(url, filename=cache_entry)
    return(Dataset(cache_entry))


def reverse_calculate_date(total_days):
    year = total_days // 365
    remaining_days = total_days % 365
    start_of_year = datetime(year, 1, 1)
    date = start_of_year + timedelta(days=remaining_days - 1)
    return date.date()

def get_actual_time(rawtime):
    start_value = int(rawtime)
    actual_date = reverse_calculate_date(start_value)
    
    return actual_date

def _get_cmip6_data_from_stac(model, scenario, variable, start_date, end_date, lb, ub):
    result = None
    result_days = (end_date - start_date).days + 1
    if have_pc:
        search = catalog.search(
                collections=["nasa-nex-gddp-cmip6"],
                datetime=f'{start_date}/{end_date}',
                query = {
                    "cmip6:model": {
                        "eq": model
                    },
                    "cmip6:scenario": {
                        "in": ['historical', scenario]
                    },  
                },
                sortby=[{'field':'cmip6:year','direction':'asc'}]
        )
        items = search.item_collection()
            
    for item in items:
        if have_pc:
            year = item.properties['cmip6:year']
            url = item.assets[variable].href
            ds = _get_dataset(url)
        else:
            pass
        data = ds[variable]
        if result is None:
            if lb[0] >= data[0].shape[0] or lb[1] >= data[0].shape[1]:
                return None
            ub = (min(ub[0]+1, data[0].shape[0]), min(ub[1]+1, data[0].shape[1]))
            shape = (result_days, ub[0] - lb[0], ub[1] - lb[1])
            result = np.ndarray(shape, dtype = data.dtype)
        item_start = max(start_date, date(year, 1,1))
        item_end = min(date(year,12,31), end_date)
        start_gidx = (item_start - start_date).days
        end_gidx = (item_end - start_date).days + 1
        start_iidx = (item_start - date(year, 1 , 1)).days
        end_iidx = (item_end - date(year, 1, 1)).days + 1
        result[start_gidx:end_gidx,:,:] = data[start_iidx:end_iidx,lb[0]:ub[0],lb[1]:ub[1]]
    return(result)


def _get_gddp_params(name):
    model = 'ACCESS-CM2'
    scenario = 'ssp585'
    variable = 'tas'
    var_name = name.split('\\')[-1]
    quality = 0
    lb1,lb2=0,0
    ub1,ub2 = 1399,599

    name_parts = var_name.split(',')
    for part in name_parts:
        if part[0] == 'm':
            model = part[2:]
            if have_pc and model not in model_list:
                raise ValueError(f"model {model} not available.") 
        if part[0] == 's':
            scenario = part[2:]
            if have_pc and scenario not in scenario_list:
                raise ValueError(f"scenario {scenario} not available.")
        if part[0] == 'v':
            variable = part[2:]
            if have_pc and variable not in variable_list:
                raise ValueError(f"variable {variable} not available.")
        if part[0] == 'q':
            quality = int(part[2:])
        if part[0] == 'l':
            lbb= part[2:]
            lb1,lb2=split_str(lbb)
        if part[0] == 'u':
            ubb= part[2:]
            ub1,ub2=split_str(ubb)
        if part[0] == 't':
            time= part[2:]
            t1,t2=split_str(time)



    if variable == None:
        raise ValueError('No variable name specified')
    print(model, scenario, variable, quality,t1,t2,lb1,lb2,ub1,ub2)
    return model, scenario, variable, quality,t1,t2,lb1,lb2,ub1,ub2


def _get_idx_data(dataset_name,t1,quality, lb1, ub1,lb2,ub2,server_location='atlantis'):
    error_type="IDX_NOT_FOUND"
    print(f'Looking for data at {server_location}')
    if server_location=='atlantis':
        db = ov.LoadDataset(f"http://atlantis.sci.utah.edu/mod_visus?dataset={dataset_name}&cached=arco")
    
    if server_location=='sealstorage':
        db = ov.LoadDataset(f"http://atlantis.sci.utah.edu/mod_visus?dataset={dataset_name}&cached=arco")
    error_type="PARAM_NOT_FOUND"
    sys.stdout.flush()
    print('IDX loaded...')
    sys.stdout.flush()
    error_type="PARAM_NOT_FOUND"
    data=db.read(time=t1,quality=quality,x=[lb1,ub1],y=[lb2,ub2])
    error_type="NONE"
    return data

def _create_idx_data(dataset_name, ub1,ub2, location='local'):
    if (location=="local"):
        db=ov.CreateIdx(
            
            url=f'{dataset_name}.idx',
            fields=dataset_name,
            time=[entire_time_start, entire_time_end, 'time_%d/'],
            dims=[ub2,ub1],
            arco=ARCO
        )
        print('New IDX created')
    return True

def _write_idx_data(dataset_url,data,time_start, time_end, lb0,lb1,ub0,ub1):
    
    db=ov.LoadDataset(dataset_url)
    counter=0
    for t in range(time_start, time_end):
        db.write(data[counter], time=t,x=[lb1,ub1],y=[lb2,ub2])
        counter=+1
    print('Writing to IDX completed')
    return True


def _get_cmip6_data(model, scenario, variable, quality, t1, t2, lb1, lb2, ub1, ub2):
    dataset_name = f"{variable}_day_{model}_{scenario}_r1i1p1f1_gn"
    print(dataset_name)
    error_type = "NONE"
    sys.stdout.flush()
    try:
        print('Checking for IDX files...')
        data = _get_idx_data(dataset_name, t1, quality,lb1, ub1, lb2, ub2)
    except Exception as e:
        print('Error with IDX file:', e)
        print('Fetching data from Microsoft STAC now...')
        sys.stdout.flush()
        actual_start_date = get_actual_time(t1)
        actual_end_date = get_actual_time(t2)
        data = _get_cmip6_data_from_stac(model, scenario, variable, actual_start_date, actual_end_date, (lb1, lb2), (ub1, ub2))
        print("Retrieved data size from STAC")
        print(type(data))
        sys.stdout.flush()
        
        
        if len(data) != 0:      
                def create_and_write_idx():
                    try:
                        if error_type == "IDX_NOT_FOUND":
                            print("Starting IDX creation in the background...")
                            sys.stdout.flush()
                            test_idx_create = _create_idx_data(dataset_name, orig_ub[0], orig_ub[1] + 1)
                            sys.stdout.flush()
                            if test_idx_create:
                                _write_idx_data(dataset_name, data, t1, t2, lb1, lb2, ub1, ub2)
                        else:
                            _write_idx_data(dataset_name, data, t1, t2, lb1, lb2, ub1, ub2)
                        print('IDX creation and writing completed.')
                        sys.stdout.flush()
                    except Exception as idx_err:
                        print('Creating or writing to IDX failed:', idx_err)
                        sys.stdout.flush()

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(create_and_write_idx)

    return np.array(data)


def query(name, version, lb, ub):
    model, scenario, variable, quality,t1,t2,lb1,lb2,ub1,ub2 = _get_gddp_params(name)
    result = _get_cmip6_data( model, scenario, variable, quality,t1,t2,lb1,lb2,ub1,ub2)
    sys.stdout.flush()
    return result

if __name__ == '__main__':
    s = date(2013, 5, 2)
    e = date(2013, 5, 2)
    start = (s - base_date).days
    span = (e - s).days
    orig_lb = (0,0)
    orig_ub = (599,1399)
    entire_time_start=711750
    entire_time_end=766865
    ARCO='2mb'
    version = pack('uint:16, uint:16', start, span).uint
    res = query(name='cmip6-planetary\\m:ACCESS-ESM1-5,v:tas', version=1, lb=orig_lb, ub=orig_ub)
    print(res.shape)
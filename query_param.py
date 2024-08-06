import planetary_computer
from netCDF4 import Dataset
import fsspec
import pystac_client
import numpy as np
import os
from urllib.parse import urlparse
from urllib.request import urlretrieve
from bitstring import Bits, pack
from datetime import date, timedelta
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
def _get_gddp_params(name):
    model = 'ACCESS-CM2'
    scenario = 'ssp585'
    variable = 'tas'
    var_name = name.split('\\')[-1]
    quality = 0
    lb1,lb2=0,0
    ub1,ub2 = 599,1399

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
        if part[0] == 'lb':
            lbb= int(part[2:])
            lb1,lb2=split_str(lbb)
        if part[0] == 'ub':
            ubb= int(part[2:])
            ub1,ub2=split_str(ubb)
        if part[0] == 't':
            time= int(part[2:])
            t1,t2=split_str(time)



    if variable == None:
        raise ValueError('No variable name specified')
    return model, scenario, variable, quality,t1,t2,lb1,lb2,ub1,ub2



def _get_cmip6_data( model, scenario, variable, quality,t1,t2,lb1,lb2,ub1,ub2):


    dataset_name = f"{variable}_day_{model}_{scenario}_r1i1p1f1_gn"
    print(dataset_name)
    sys.stdout.flush()
    db = ov.LoadDataset(f"http://atlantis.sci.utah.edu/mod_visus?dataset={dataset_name}&cached=arco")
    print('IDX loaded')
    data=db.read(time=t1,quality=quality,x=[lb1,ub1],y=[lb2,ub2])
    print('Data Read complete, Max Data:')
    result = data
    return np.array(result)

def query(name, version, lb, ub):
    print('GETTING RESULT HERE-------------------------------------------')
    model, scenario, variable, qualityt1,t2,lb1,lb2,ub1,ub2 = _get_gddp_params(name)
    result = _get_cmip6_data( model, scenario, variable, quality)
    sys.stdout.flush()
    return result

if __name__ == '__main__':
    print('MAIN CALLED-------------------------------------------------------')
    s = date(2013, 5, 2)
    e = date(2013, 5, 2)
    start = (s - base_date).days
    span = (e - s).days
    lb = (0,0)
    ub = (599,1399)
    version = pack('uint:16, uint:16', start, span).uint
    res = query(name='cmip6-planetary\\m:ACCESS-ESM1-5,v:tas', version=1, lb=lb, ub=ub)
    print(res.shape)
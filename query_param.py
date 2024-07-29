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


def _get_cmip6_data():
    model = "ACCESS-CM2"
    variable  = "tas" 

    year = 2020 
    # 2015 is the year whne the data switches from historical to simulated
    scenario = "historical" if year < 2015 else "ssp585"

    # Open (connect to) dataset
    dataset_name = f"{variable}_day_{model}_{scenario}_r1i1p1f1_gn"
    print(dataset_name)
    db = ov.LoadDataset(f"http://atlantis.sci.utah.edu/mod_visus?dataset={dataset_name}&cached=arco")

    day_of_the_year = 202 
    timestep =year*365 + day_of_the_year
    quality = -8 
    data=db.read(time=timestep,quality=quality)
    result = [data,data,data]
    return np.array(result)

def query(name, version, lb, ub):
    print('GETTING RESULT HERE-------------------------------------------')
    # result = _get_cmip6_data()
    result=np.random.rand(7,30,40)
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
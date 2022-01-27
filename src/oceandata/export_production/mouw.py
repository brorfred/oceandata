"""
Global ocean particulate organic carbon flux.


Ref: https://doi.org/10.1594/PANGAEA.855594,
"""
import os, pathlib
import warnings

import pandas as pd
import numpy as np
import requests

DATADIR = pathlib.PurePath(pathlib.Path.home(), ".oceandata")
pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)
DATAURL = "https://doi.pangaea.de/10.1594/PANGAEA.855594"

"""
def load():
    df = pd.read_hdf("h5files/ep_mouw_with_sat.h5")
    df["Zeu"] = 4.6/df.kd490
    df["ep_obs"] = df.POC_flux
    df["chl"] = df["chl"] * df["Zeu"]

    #lh = ecoregions.Longhurst()
    #longh = lh.match("regions", lonvec=dfm.lon, latvec=dfm.lat, jdvec=dfm.lat*0)
    #dfm["longhurst"] = longh
    return df
"""

def load(datadir=DATADIR, filename="GO_flux.tab", with_std=False):
    """Load tab file and fix some columns"""
    fn = os.path.join(datadir, filename)
    if not os.path.isfile(fn):
        download(datadir=datadir, filename=filename)
    with open(fn ,"r") as fH: 
        while 1: 
            line = fH.readline() 
            if "*/" in line: 
                break
        df = pd.read_csv(fH, sep="\t", parse_dates=[1,])

    if not with_std:
        df.drop(columns=['Flux std dev [±]', 'C flux [mg/m**2/day]', 
                 'C flux std dev [±]', 'POC flux std dev [±]',
                 'PIC flux std dev [±]', 'PON flux std dev [±]',
                 'POP flux std dev [±]', 'PSi flux std dev [±]',
                 'PAl std dev [±]', 'CaCO3 flux std dev [±]',
                 'Reference'], inplace=True)

    df.rename(columns={'ID (Reference identifier)':"ref_ID",
                       'ID (Unique location identifier)':"UUID",
                       'Type (Data type)':"sampling_type", 
                       'Latitude':"lat", 
                       'Longitude':"lon",
                       'Flux tot [mg/m**2/day]':"tot_flux",
                       'POC flux [mg/m**2/day]':"POC_flux",
                       'PIC flux [mg/m**2/day]':"PIC_flux",
                       'PON flux [mg/m**2/day]':"PON_flux",
                       'POP flux [mg/m**2/day]':"POP_flux",
                       'PSi flux [mg/m**2/day]':"PSi_flux",
                       'PSiO2 flux [mg/m**2/day]':"PSiO2_flux",
                       'PSi(OH)4 flux [mg/m**2/day]':"PSiOH4_flux",
                       'PAl [mg/m**2/day]':"PAl_flux",
                       'Chl flux [mg/m**2/day]':"Chl_flux",
                       'Pheop flux [µg/m**2/day]':"Pheop_flux",
                       'CaCO3 flux [mg/m**2/day]':"CaCO3_flux",
                       'Fe flux [mg/m**2/day]':"Fe_flux",
                       'Mn flux [µg/m**2/day]':"Mn_flux",
                       'Ba flux [µg/m**2/day]':"Ba_flux",
                       'Detrital flux [mg/m**2/day]':"Detr_flux",
                       'Ti flux [µg/m**2/day]':"Ti_flux",
                       'Bathy depth [m] (ETOPO1 bathymetry)':"bathy",
                       'Depth water [m] (Sediment trap deployment depth)':"depth",
                       'Area [m**2]':"area",
                       'Duration [days]':"duration",
                       'Date/Time (Deployed)':"start_time",
                       'Date/time end (Retrieved)':"end_time",
                       'Area [m**2] (Surface area of trap)':"trap_area",
                        },
        inplace=True)
    df.drop(columns=['Type (Sediment trap type)', 
                     'Elevation [m a.s.l.] (Total water depth)'],
        inplace=True)
    df["start_time"] = pd.DatetimeIndex(df["start_time"])
    df["end_time"] = pd.DatetimeIndex(df["end_time"])
    df.set_index("end_time", inplace=True)
    return df

def download(datadir=DATADIR, filename="GO_flux.tab"):
    """Download txt file from BATS server
    
    Refs
    ----
    """
    local_filename = os.path.join(datadir, filename)
    try:
        os.unlink(local_filename)
    except FileNotFoundError:
        pass
    try:
        r = requests.get(DATAURL, stream=True, timeout=6, params={"format":"textfile"})
    except requests.ReadTimeout:
        warnings.warn("Connection to server timed out.")
        return False
    if r.ok:
        if local_filename is None:
            return r.text
        else:
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk:
                        f.write(chunk)
                        f.flush()
    else:
        raise IOError(f"Could not download file from server, Error {r.status_code}")

"""Mattei: Collection and analysis of a global marine phytoplankton 
primary-production dataset

Most of the available primary-production datasets are scattered in various 
repositories, reporting heterogeneous information and missing records. We 
decided to retrieve field measurements of marine phytoplankton production from 
several sources and create a homogeneous and ready-to-use dataset. We handled 
missing data and added variables related to primary production which were not 
present in the original datasets. Subsequently, we performed a general analysis 
highlighting the relationships between the variables from a numerical and an 
ecological perspective.

Data paucity is one of the main issues hindering the comprehension of complex 
natural processes. We believe that an updated and improved global dataset, 
complemented by an analysis of its characteristics, can be of interest to anyone
studying marine phytoplankton production and the processes related to it. The 
dataset described in this work is published in the PANGAEA repository 
(https://doi.org/10.1594/PANGAEA.932417) (Mattei and Scardi, 2021).



Data url:
DOI: 10.1594/PANGAEA.932417

References:
DOI: 10.5194/essd-13-4967-2021
"""
import glob
import os
import io
import pathlib
import zipfile
import warnings

import numpy as np
import pandas as pd
import requests
from datetime import datetime

DATADIR = pathlib.PurePath(pathlib.Path.home(), ".oceandata")
pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)
DATAURL = "https://download.pangaea.de/dataset/932417/files"

def load(datadir=DATADIR, 
         filename="Global_marine_phytoplankton_production_dataset.txt"):
    """Load tab file and fix some columns"""
    fn = os.path.join(datadir, filename)
    if not os.path.isfile(fn):
        download(datadir=datadir, filename=filename)
    with open(fn ,"r") as fH:
        end = fH.seek(0, 2)
        fH.seek(0)
        string = "" #"\t".join(fH.readline().split("\t")[6:])
        cnt = 0
        while fH.tell() < end:
            cnt += 1
            part = fH.readline().split("\t")
            if len(part) > 5: 
                line = "\t".join(part[len(part)-43:])
            else:
                line = ""
            string += line #"\t".join(line.split("\t")[6:])
        fstr = io.StringIO(string)
    df = pd.read_csv(fstr, sep="\t", parse_dates=True, index_col=0, 
                     dayfirst=True) 

    [df.drop(columns=key, inplace=True) for key in df.keys() 
        if "magnitude" in key]

    df.rename(columns={"SST (°C)":"sst", 
                       "Latitude":"lat", "Longitude":"lon",
                       'Bottom depth (m)':"bathy",
                       'Sampling depth (m)':"depth",
                       'Max sampling depth (m)':"depth_max",
                       'Max production depth (m)':"depth_PP_max",
                       'Mixed Layer Depth (m)':"mld",
                       'Distance from coastline (Km)':"distance_from_coast",
                       'Euphotic zone depth (m)':"Zeu",
                       "SST (°C)":"sst",
                       'Pbopt (mg C mg Chla-1 h-1)':"Pb_opt",
                       'surface PAR (E m^-2 day^-1)':"PAR_surface",
                       'Depth-resolved chl a (mg m^-3)':"chl",
                       'Total Chl a (mg m^-2)':"Tchl",
                       'Depth-integrated chl a (mg m^-2)':"chl_m2",
                       'Depth-resolved primary production (mg C m^-3 day^-1)':"PP_m3",
                       'Depth-integrated primary production (mg C m^-2 day^-1)':"PP",
                       'Production to biomass ratio (mg C day-1 / mg Chl a)':"PP/chl",
                       }, inplace=True)
    df.drop(['Year', 'Month', 'Day of the year',
             'Bottom depth sd (m)',
             'Northern hemisphere season',
             'PAR_flag', 'SST_flag',
             'hemisphere'], axis=1, inplace=True)
    df.loc[df.lon<-180, "lon"] = 360 + df.lon[df.lon<-180]
    return df
    #df.to_hdf("h5files/mattei_pp_global_data_clean.h5", "df")

def download(datadir=DATADIR, 
             filename="Global_marine_phytoplankton_production_dataset.txt"):
    """Download txt file from BATS server
    
    Refs
    ----
    """
    local_filename = os.path.join(datadir, filename)
    try:
        os.unlink(local_filename)
    except FileNotFoundError:
        pass
    url = f"{DATAURL}/{filename}"
    try:
        r = requests.get(url, stream=True, timeout=6)
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

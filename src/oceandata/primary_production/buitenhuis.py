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
DATAURL = "http://greenocean-data.uea.ac.uk/biogeochemistry"

def load(datadir=DATADIR, 
         filename="PP_Buitenhuisetal2013.xls"):
    """Load excel file and convert to pandas dataframe"""
    fn = os.path.join(datadir, filename)
    if not os.path.isfile(fn):
        download(datadir=datadir, filename=filename)
    df = pd.read_excel(fn)
    yearlist = [val if isinstance(val, int) else int(val[1:]) for val in list(df.Year)]
    df.Year = yearlist
    df = df[["Day", "Month", "Year", "LAT", "LONG", "Depth", "PP"]]
    df["date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
    df.drop(columns=["Year","Month","Day"], inplace=True)
    df.rename(columns={"LAT":"lat", "LONG":"lon","Depth":"depth"}, inplace=True)
    mask = df["PP"].apply(lambda x: isinstance(x, str))
    df = df[~mask]
    df = df[df.PP>0]
    df.set_index("date", inplace=True)
    return df

def download(datadir=DATADIR, 
             filename="PP_Buitenhuisetal2013.xls"):
    """Download Excel file from UEA server
    
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

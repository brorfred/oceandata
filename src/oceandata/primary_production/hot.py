
import glob
import os
import io
import pathlib
import warnings
import ftplib
from urllib.parse import urlsplit


import numpy as np
import pandas as pd
import requests
from datetime import datetime
import click
from bs4 import BeautifulSoup

DATADIR = pathlib.PurePath(pathlib.Path.home(), ".oceandata/HOT/pp")
pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)
DATAURL = "https://hahana.soest.hawaii.edu/FTP/hot/primary_production/"
DATAEXT = "pp"

def filelist():
    return list(pathlib.Path(DATADIR).glob("hot*.pp"))

def listFD(url=None, ext=None):
    url = DATAURL if url is None else url
    ext = DATAEXT if ext is None else ext
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]

def download(fileurl):
    """Download txt file from BATS server
    
    Refs
    ----
    """
    local_filename = DATADIR / fileurl.split("/")[-1]

    try:
        os.unlink(local_filename)
    except FileNotFoundError:
        pass
    try:
        r = requests.get(fileurl, stream=True, timeout=6)
    except requests.ReadTimeout:
        warnings.warn("Connection to server timed out.")
        return False
    if r.ok:
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk:
                    f.write(chunk)
                    f.flush()
    else:
        raise IOError(f"Could not download file from server, Error {r.status_code}")


def generate_h5_file():
    """Generate h5 file by downloading and concating pp files."""
    with click.progressbar(listFD()) as urls:
        for url in urls:
            download(url)
    dflist = [read_pp(p) for p in filelist()]
    df = pd.concat(dflist)
    df.to_hdf(DATADIR / "pp_hot.h5", "df")

def read_pp(filename):
    names = ["cruise_ID", "inc",
             "time", "date", "time_start", "time_end", "depth",
             "Chl_Mean", "Chl_SD", "Pheo_Mean", "Pheo_SD",
             "light1", "light2", "light3",
             "dark1", "dark2", "dark3", "salt",
             "Prochl",  "Hetero", "Synecho", "Euk", "flags"]
    df = pd.read_csv(filename, skiprows=7, sep=" ", 
                     skipinitialspace=True, names=names, na_values=-9, 
                     index_col=False, converters={'date': str})
    df["pp_light"] = np.nanmean(df[["light1", "light2", "light3"]], axis=1)
    df["pp_dark"] = np.nanmean(df[["dark1", "dark2", "dark3"]], axis=1)
    df["pp_obs"] = df["pp_light"] - df["pp_dark"]
    df.loc[df.pp_obs<0, "pp_obs"] = 0
    dtmvec = pd.to_datetime(df.date, format="%y%m%d")
    df.set_index(pd.DatetimeIndex(dtmvec), inplace=True)
    df.drop(columns=["light1", "light2", "light3", 
                     "dark1", "dark2", "dark3", "date"],
            inplace=True)
    return df


def load(datadir=DATADIR, filename="pp_hot.h5"):
    """Load tab file and fix some columns"""
    if not os.path.isfile(DATADIR / filename):
        generate_h5_file()
    return pd.read_hdf(DATADIR / filename)



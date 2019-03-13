
import os
"""MAPPS: photosynthesis-irradiance parameters for marine phytoplankton

The MAPPS global database of photosynthesis-irradiance (P-E) parameters consists of over 5000 P-E experiments that provides information on the spatio-temporal variability in the two P-E parameters (the assimilation number, and the initial slope) that are fundamental inputs for models of marine primary production that use chlorophyll as the state variable. The experiments were carried out by an international group of research scientists to examine the basin-scale variability in the photophysiological response of marine phytoplankton over a range of oceanic regimes (from the oligotrophic gyres to productive shelf systems) and covers several decades. These data can be used to improve the assignment of P-E parameters in the estimation of marine primary production using satellite dat

Data DOI:
DOI:10.1594/PANGAEA.874087

References:
DOI:10.5194/essd-10-251-2018

"""
import glob
import pathlib

import numpy as np
import pylab as pl
import pandas as pd

import requests

DATADIR = "/Users/bror/.oceandata/"
FILENAME = "Bouman_2017.tab.tsv"
pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)

def load(filename=FILENAME):
    """Load tsv file and fix some columns"""
    df = pd.read_csv(os.path.join(DATADIR, FILENAME), sep="\t", skiprows=57)
    df["lat"]    = df["Latitude"]
    df["lon"]    = df["Longitude"]
    df["region"] = df["BG province"]
    df["depth"]  = df["Depth water [m]"]
    df["chl"]    = df["Chl a [µg/l]"]
    df["alpha"]  = df["alpha [(mg C/mg Chl a/h)/(µE/m**2/s)]"] 
    df["PBmax"]  = df["PBmax [mg C/mg Chl a/h]"]
    df["Ek"]     = df["Ek [µmol/m**2/s]"]
    del df["Latitude"], df["Longitude"], df["BG province"]
    del df["Depth water [m]"], df["Chl a [µg/l]"]
    del df["alpha [(mg C/mg Chl a/h)/(µE/m**2/s)]"]
    del df["PBmax [mg C/mg Chl a/h]"], df["Ek [µmol/m**2/s]"]
    df.set_index(pd.DatetimeIndex(df["Date/Time"]), inplace=True)
    del df["Date/Time"]
    return df


def download(url="https://doi.pangaea.de/10.1594/PANGAEA.874087",
            params={"format":"textfile"}):
    """Download tsv file from Pangaea server"""
    local_filename = os.path.join(DATADIR, FILENAME)
    try:
        r = requests.get(url, params=params, stream=True, timeout=2)
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
            return None
    else:
        raise IOError("Could not download file from server")

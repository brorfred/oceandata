"""valente: photosynthesis-irradiance parameters for marine phytoplankton

A global compilation of in situ data is useful to evaluate the quality of 
ocean-colour satellite data records. Here we describe the data compiled for
the validation of the ocean-colour products from the ESA Ocean Colour 
Climate Change Initiative (OC-CCI). The data were acquired from several 
sources (including, inter alia, MOBY, BOUSSOLE, AERONET-OC, SeaBASS, NOMAD,
MERMAID, AMT, ICES, HOT, GeP&CO) between 1997 and 2017. Observations of the
following variables were compiled: spectral remote-sensing reflectances,
concentrations of chlorophyll-a, spectral inherent optical properties,
spectral diffuse attenuation coefficients and total suspended matter.
The data were from multi-project archives acquired via open internet
services or from individual projects, acquired directly from data providers.
Methodologies were implemented for homogenisation, quality control and merging
of all data. No changes were made to the original data, other than averaging
of observations that were close in time and space, elimination of some points
after quality control and conversion to a standard format. The final result
is a merged table designed for validation of satellite-derived ocean-colour
products and available in text format. Metadata of each in situ measurement
(original source, cruise or experiment, principal investigator) were propagated
throughout the work and made available in the final table. By making the
metadata available, provenance is better documented and it is also possible
to analyse each set of data separately. This paper also describes the changes
that were made to the compilation in relation to the previous version.

Data DOI:
DOI:10.1594/PANGAEA.898188

References:
DOI:10.5194/essd-8-235-2016

"""
import glob
import os
import pathlib
import zipfile
import warnings

import numpy as np
import pandas as pd
import requests

DATADIR = os.path.expanduser("~/.oceandata/valente_iop")
pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)
FILENAME = "valente_2019.zip"

def load_chl(filename="insitudb_chla.tab"):
    """Load tab file and fix some columns"""
    fn = os.path.join(DATADIR, "datasets", filename)
    with open(fn ,"r") as fH: 
        while 1: 
            line = fH.readline() 
            if "*/" in line: 
                break
        df = pd.read_csv(fH, sep="\t", parse_dates=[1,])
    df["lat"] = df["Latitude"]
    df["lon"] = df["Longitude"]
    df["chl_hpcl"] = df["Chl a [mg/m**3] (High Performance Liquid Chrom...)"]
    df["chl_fluo"] = df["Chl a [mg/m**3] (Chlorophyll a, fluorometric o...)"]
    del df["Chl a [mg/m**3] (High Performance Liquid Chrom...)"]
    del df["Chl a [mg/m**3] (Chlorophyll a, fluorometric o...)"]
    del df["Longitude"], df["Latitude"]
    df.set_index("Date/Time", inplace=True)
    return df


def download(url="https://doi.pangaea.de/10.1594/PANGAEA.898188",
             params={"format":"zip"}, filename=None):
    """Download tsv file from Pangaea server
    
    Refs
    ----
    Zipfile https://stackoverflow.com/questions/3451111/unzipping-files-in-python

    """
    filename = FILENAME if filename is None else filename
    local_filename = os.path.join(DATADIR, filename)
    try:
        os.unlink(local_filename)
    except FileNotFoundError:
        pass

    try:
        r = requests.get(url, params=params, stream=True, timeout=6)
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
        raise IOError("Could not download file from server")

    with zipfile.ZipFile(local_filename,"r") as zip_ref:
        zip_ref.extractall(DATADIR)


def download_pml(url="https://github.com/brorfred/oceandata/raw/master/data/",
                filename="GLOBAL_PE_W_LOV_2019.csv", params={}):
    """Download local PML version of MAPPS"""
    download(url=f"{url}/{filename}", filename=filename, params=params)


def load_pml(filename="GLOBAL_PE_W_LOV_2019.csv"):
    fn = os.path.join(DATADIR, filename)
    df = pd.read_csv(fn, parse_dates=[[4,5,6]], index_col="YEAR_MONTH_DAY")
    df = df.rename(columns={"LAT":"lat", "LON":"lon", "DEPTH":"depth",
                            "TEMP":"temp", "TCHL":"chl", "ALPHA":"alpha",
                            'NITRATE':"NO3",'SILICATE':"Si4",'PHOSPHATE':"PO4",
                            "PMB":"PBmax", "EK":"Ek","PROVNUM":"region"})
    return df

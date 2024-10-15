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
import os
import pathlib

import numpy as np
import pandas as pd

from ..chl.valente import DATADIR, download

pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)

def load(datadir=DATADIR, filename="insitudb_rrs_satbands6_V3.tab"):
    """Load tab file and fix some columns"""
    fn = os.path.join(datadir, "datasets", filename)
    if not os.path.isfile(fn):
        download(datadir=datadir)
    with open(fn ,"r") as fH: 
        while 1: 
            line = fH.readline() 
            if "*/" in line: 
                break
        df = pd.read_csv(fH, sep="\t", parse_dates=[1,])
    df = df.rename(columns={"Latitude":"lat", "Longitude":"lon"})
    #df["lat"] = df["Latitude"]
    #df["lon"] = df["Longitude"]
    #del df["Longitude"], df["Latitude"]
    df.set_index("Date/Time", inplace=True)
    return df
    df["chl_hpcl"] = df["Chl a [mg/m**3] (High Performance Liquid Chrom...)"]
    df["chl_fluo"] = df["Chl a [mg/m**3] (Chlorophyll a, fluorometric o...)"]
    del df["Chl a [mg/m**3] (High Performance Liquid Chrom...)"]
    del df["Chl a [mg/m**3] (Chlorophyll a, fluorometric o...)"]
    return df

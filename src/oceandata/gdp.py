
"""GDP: The global drifter program

Reference:
https://www.aoml.noaa.gov/phod/gdp/

"""
import os
import glob
import pathlib
import ftplib
from urllib.parse import urlsplit



import numpy as np
import pandas as pd

import requests
import click

DATADIR = os.path.expanduser("~/.oceandata/")
pathlib.Path(DATADIR).mkdir(parents=True, exist_ok=True)

def read_dat(filename, sst=False, vel=False, var=False):
    df = pd.read_csv(filename, sep=" ", skipinitialspace=True,
                     compression='gzip', na_values=999.999,
                     names=["id", "month", "day", "year", "lat", "lon",
                            "sst", "vel_east", "vel_north", "speed",
                            "var_lat", "var_lon", "var_temp"])
    df["hour"] = ((df.day - df["day"].astype(int)) * 24).astype(np.int32)
    df.set_index(pd.to_datetime(df[["year","month","day","hour"]]),inplace=True)
    del df["year"], df["month"], df["day"], df["hour"]
    df.loc[df["lon"]>180, "lon"] = df.loc[df["lon"]>180, "lon"] - 360
    if not sst:
        del df["sst"]
    if not vel:
        del df["vel_east"], df["vel_north"], df["speed"]
    if not var:
        del df["var_lat"], df["var_lon"], df["var_temp"]
    return df

def load(v1=None, sst=False, vel=False, var=False):
    """Load gzipped dat file to a pandas dataframe"""
    if v1 is None:
        dflist = []
        for v1 in [1, 5001, 10001, 15001]:
            dflist.append(load(v1, sst=sst, vel=vel, var=var))
        return pd.concat(dflist)
    if   v1 == 1:
        v2 = 5000
    elif v1 == 5001:
        v2 = 10000
    elif v1 == 10001:
        v2 = 15000
    elif v1 == 15001:
        v2 = "mar20"
    filename = os.path.join(DATADIR, f"buoydata_{v1}_{v2}.dat.gz")
    h5filename = filename[:-7] + ".h5"
    if os.path.isfile(h5filename):
        return pd.read_hdf(h5filename)
    if not os.path.isfile(filename):
        print("Downloading file")
        download(v1=v1, v2=v2)
    df = read_dat(filename, sst=sst, vel=vel, var=var)
    df.to_hdf(h5filename, "df")
    return df

def vprint(text):
    pass
    #print(text)

def open_ftp_session(url="ftp://ftp.aoml.noaa.gov/phod/pub/buoydata/"):
    spliturl = urlsplit(url)
    try:
        ftp = ftplib.FTP(spliturl.netloc) 
        vprint(ftp.login("anonymous", "oceandata@bror.us"))
        ftpdir = spliturl.path
        vprint("Change dir to '%s'" % ftpdir)
        vprint(ftp.cwd(ftpdir))
    except ftplib.error_perm as err:
        print (spliturl.netloc)
        print (os.path.split(spliturl.path)[0])
        raise IOError(err)
    return ftp

def server_file_list(url="ftp://ftp.aoml.noaa.gov/phod/pub/buoydata/", ftp=None):
    pass

def download(url="ftp://ftp.aoml.noaa.gov/phod/pub/buoydata/"):
    """Download tsv file from NOOA's website using ftp"""
    lfn = filename = f"buoydata_{v1}_{v2}.dat.gz"
    ftp = open_ftp_session(url=url)

    local_filename = os.path.join(DATADIR, filename)
    if not filename in ftp.nlst():
        print(ftp.nlst())
        raise ftplib.Error("'%s' is not the ftp server" % lfn)
    with open(local_filename, 'wb') as lfh:
        ftp.voidcmd('TYPE I')
        length = ftp.size(lfn)
        short_lfn = lfn if len(lfn)<18 else lfn[:4] + "..." + lfn[-13:]
        with click.progressbar(length=length, label=short_lfn) as bar:
            def file_write(data):
                lfh.write(data) 
                bar.update(len(data))
            try:
                ftp.retrbinary("RETR %s" % lfn, file_write)
            except ftplib.error_perm as err:
                os.unlink(local_filename)
                raise IOError(err)
        ftp.quit()

def unzip(filename="SOCATv6.tsv.zip"):
    local_filename = os.path.join(DATADIR, filename)
    zip_ref = zipfile.ZipFile(local_filename, 'r')
    zip_ref.extractall(DATADIR)
    zip_ref.close()

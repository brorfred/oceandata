import numpy as np
import pandas as pd

def read_buitenhuis():
    df = pd.read_hdf("buiten_pp_longhurst.h5")
    df.set_index(pd.DatetimeIndex(df.date), inplace=True)
    df["sst"] = ostia.match(df.lon,df.lat,df.index)

    cc = cci.LocalPML(ver="5.0")
    jdvec = date2num(df.date)
    mask = df.index>="1998-01-01"
    df["chl"] = np.nan
    df.loc[mask, "chl"] = cc.match(
        "chl", df.lon[mask], df.lat[mask], jdvec[mask], nei=9)
    df["kd_490"] = np.nan
    df.loc[mask, "kd_490"] = cc.match(
        "kd490", df.lon[mask], df.lat[mask], jdvec[mask], nei=9)
    df["Zeu"] = 4.6 / df["kd_490"]

    ns = nasa.MODIS(res="4km")
    mask = df.index>="2003-01-01"
    df["par"] = np.nan
    df.loc[mask, "par"] = ns.match("par", df.lon[mask], df.lat[mask], jdvec[mask])
    df["month"] = df.index.month
    df.to_hdf("h5files/buiten_pp_longhurst_sat.h5", "df")
    return df



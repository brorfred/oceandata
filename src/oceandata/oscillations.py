
import numpy as np
import pandas as pd

def read_psl(dataurl, skipfooter=4):
    df = pd.read_csv(dataurl, sep=" ", na_values=-999.0,
                     skiprows=1, skipfooter=skipfooter, 
                     index_col="year", skipinitialspace=True,
                     names=["year",1,2,3,4,5,6,7,8,9,10,11,12],
                     engine='python')
    df = df.unstack().reset_index(level=[0,1]).set_index("year").rename(
        columns={"level_0":"month", 0:"value"})
    df = df.sort_values(by=["year","month"]) 
    dtm = [pd.to_datetime(f"{year}-{month}-01")for year, month in zip(df.index,df.month)]
    return df.set_index(pd.DatetimeIndex(dtm))

def read_enso():
    dataurl = "https://psl.noaa.gov/enso/mei/data/meiv2.data"
    df = read_psl(dataurl, skipfooter=4)
    return df.rename(columns={"value":"enso"})

def read_iod():
    dataurl = "https://psl.noaa.gov/gcos_wgsp/Timeseries/Data/dmi.had.long.data"
    df = read_psl(dataurl, skipfooter=7)
    return df.rename(columns={"value":"iod"})

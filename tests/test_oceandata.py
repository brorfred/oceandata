from oceandata import __version__

import tempfile

import pandas as pd
import oceandata

def test_version():
    assert __version__ == '0.3.1'

def assert_dataframe(df):
    assert "lat" in df
    assert "lon" in df
    assert isinstance(df.index, pd.DatetimeIndex)

def test_mapps_download():
    from oceandata import mapps
    mapps.download()

def test_mapps_load():
    from oceandata import mapps
    df = mapps.load()

def test_mapps_download_pml():
    from oceandata import mapps
    mapps.download_pml()

def test_mapps_load_pml():
    from oceandata import mapps
    df = mapps.load_pml()

def test_valente():
    from oceandata.chl import valente
    df = valente.load()
    with tempfile.TemporaryDirectory() as tmpdirname:
        df = valente.load(datadir=tmpdirname)
    assert_dataframe(df)

def test_mattei():
    from oceandata.primary_production import mattei
    df = mattei.load()
    with tempfile.TemporaryDirectory() as tmpdirname:
        df = mattei.load(datadir=tmpdirname)
    assert_dataframe(df)

def test_buitenhuis():
    from oceandata.primary_production import buitenhuis
    df = buitenhuis.load()
    with tempfile.TemporaryDirectory() as tmpdirname:
        df = buitenhuis.load(datadir=tmpdirname)
    assert_dataframe(df)

def test_mouw():
    from oceandata.export_production import mouw
    df = mouw.load()
    with tempfile.TemporaryDirectory() as tmpdirname:
        df = mouw.load(datadir=tmpdirname)
    assert_dataframe(df)
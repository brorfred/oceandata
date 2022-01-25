from oceandata import __version__

import tempfile

import oceandata

def test_version():
    assert __version__ == '0.2.2'


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

def test_mattei():
    from oceandata.primary_production import mattei
    df = mattei.load()
    with tempfile.TemporaryDirectory() as tmpdirname:
        df = mattei.load(datadir=tmpdirname)
    assert "lat" in df
    assert "lon" in df

from oceandata import __version__

import oceandata

def test_version():
    assert __version__ == '0.1.3'


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

#!/usr/bin/env python3
"""
make_bea_regions.py
Downloads Census "cartographic boundary" state polygons, assigns Bureau of Economic Analysis (BEA)
regions, dissolves to 8 regions, and writes a WGS-84 GeoJSON.

Usage:
  python make_bea_regions.py [--output bea_regions_wgs84.geojson] [--year 2022] [--scale 20m|5m]

Requires: geopandas, shapely, pyogrio or fiona, requests
"""
import argparse
import io
import sys
import zipfile
import tempfile
from pathlib import Path

import geopandas as gpd
import requests

BEA_MAP = {
    'CT':'New England','ME':'New England','MA':'New England','NH':'New England','RI':'New England','VT':'New England',
    'DE':'Mideast','DC':'Mideast','MD':'Mideast','NJ':'Mideast','NY':'Mideast','PA':'Mideast',
    'IL':'Great Lakes','IN':'Great Lakes','MI':'Great Lakes','OH':'Great Lakes','WI':'Great Lakes',
    'IA':'Plains','KS':'Plains','MN':'Plains','MO':'Plains','NE':'Plains','ND':'Plains','SD':'Plains',
    'AL':'Southeast','AR':'Southeast','FL':'Southeast','GA':'Southeast','KY':'Southeast','LA':'Southeast',
    'MS':'Southeast','NC':'Southeast','SC':'Southeast','TN':'Southeast','VA':'Southeast','WV':'Southeast',
    'AZ':'Southwest','NM':'Southwest','OK':'Southwest','TX':'Southwest',
    'CO':'Rocky Mountain','ID':'Rocky Mountain','MT':'Rocky Mountain','UT':'Rocky Mountain','WY':'Rocky Mountain',
    'AK':'Far West','CA':'Far West','HI':'Far West','NV':'Far West','OR':'Far West','WA':'Far West'
}

TERRITORIES = {'PR','GU','VI','MP','AS'}

def download_states(year: int = 2022, scale: str = "20m") -> gpd.GeoDataFrame:
    base = f"https://www2.census.gov/geo/tiger/GENZ{year}/shp"
    zip_url = f"{base}/cb_{year}_us_state_{scale}.zip"
    print(f"Downloading {zip_url} ...", file=sys.stderr)
    resp = requests.get(zip_url, timeout=60)
    resp.raise_for_status()
    zbytes = io.BytesIO(resp.content)
    with zipfile.ZipFile(zbytes) as zf:
        shp_name = next(n for n in zf.namelist() if n.endswith(".shp"))
        with tempfile.TemporaryDirectory() as td:
            zf.extractall(td)
            shp_path = Path(td) / shp_name
            # Use pyogrio if available for speed; geopandas will pick best available driver
            gdf = gpd.read_file(shp_path)
    return gdf

def build_bea_regions(states: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if 'STUSPS' not in states.columns:
        raise KeyError("Expected 'STUSPS' column in states layer (two-letter postal codes).")
    # Exclude territories and assign region
    states = states[~states['STUSPS'].isin(TERRITORIES)].copy()
    states['bea_region'] = states['STUSPS'].map(BEA_MAP)
    missing = states['bea_region'].isna().sum()
    if missing:
        bad = states.loc[states['bea_region'].isna(), 'STUSPS'].tolist()
        raise ValueError(f"Some states missing BEA region mapping: {bad}")
    # Dissolve and ensure WGS84
    bea = states.dissolve(by='bea_region', as_index=False, aggfunc='first')
    bea = bea.to_crs(4326)
    return bea[['bea_region','geometry']]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="bea_regions_wgs84.geojson",
                    help="Output GeoJSON filename")
    ap.add_argument("--year", type=int, default=2022, help="Census year (e.g., 2022)")
    ap.add_argument("--scale", choices=["5m","20m"], default="20m",
                    help="Cartographic boundary scale (5m is more detailed, larger file)")
    args = ap.parse_args()

    states = download_states(args.year, args.scale)
    bea = build_bea_regions(states)
    bea.to_file(args.output, driver="GeoJSON")
    print(f"Wrote {args.output} with {len(bea)} features.", file=sys.stderr)

if __name__ == "__main__":
    main()

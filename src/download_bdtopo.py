#!/usr/bin/env python
# coding: utf-8

# * Download de la bdtopo pour une année donnée pour les départements de la métropole de Lyon (Ain, Isère, Rhone)
# * Extraction des batiments industriels et communes dans la zone d'étude passée en entrée
# * Sauvegarde locale des fichiers
# 
# Attention, les operations de joins doivent prendre en compte l'entièreté des communes intersectées par le buffer


import geopandas as gpd 
import pandas as pd 
import os 
import numpy as np 
import folium
from shapely import Point
from py7zr import unpack_7zarchive
from pathlib import Path
import shutil
import requests
import shutil
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from src.utils import check_dir


from src.config import *


def _download(url) -> requests.Response: 
    try:
        res = requests.get(url)
    except:
        raise requests.HTTPError("url not valid")
    if res.status_code == 200:
        return res.content
    else:
        raise requests.HTTPError("Download Fail")


def download_html(url) -> requests.Response: 
    return _download(url)
    
def download_7z(url, out_dir) -> str:
    print(f"Go to to {url}")
    content =  _download(url)
    out_path = os.path.join(out_dir, os.path.basename(url))
    with open(out_path, 'wb') as out:
        out.write(content)
    print(f"{out_path} downloaded !")
    return out_path



def parse_html(content, dept: str, year: int, format="SHP") -> str:
    
    soup = BeautifulSoup(content, "html.parser")
    hrefs = [_["href"] for _ in soup.find_all('a', href=True)]
    
    code_dept = dept.zfill(3)
    pattern = f"D{code_dept}_{year}"
    
    href = [_ for _ in hrefs if (pattern in _) and (format in _) and (_.endswith(".7z"))]
    if not href: 
        raise ValueError(f"archive file not found for {dept} - {year}")
    if  len(href) > 1: 
        # select ancient update = keep old nomenclature
        tuple_href = [(Path(h).stem, h) for h in href]
        return sorted(tuple_href, key= lambda x: x[0], reverse=False)[0][1]
        
    return href[0]


def download_bdtopo(out_dir:str,
                    dept: str, 
                    year: int, 
                    url:str, 
                    format="SHP") -> str:
    
    content = download_html(url)
    
    href = parse_html(content, dept, year)
    
    out_path = os.path.join(out_dir, os.path.basename(href))
    
    if not os.path.exists(out_path):
        out_path = download_7z(href, out_dir)
    else:
        print(f"skip download for {dept} on {year}")
    return out_path



def extract_7z(arch_path, out_dir) -> str: 
    
    fname = Path(arch_path).stem
    out_path = os.path.join(out_dir, fname)
    if not os.path.exists(out_path):
    
        print(f"extration process... {arch_path}")
        if not any([_[0] == "7zip" for _ in shutil.get_unpack_formats()]):
            shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
        shutil.unpack_archive(arch_path, out_dir)
        print(f"extracted {os.path.join(out_dir, fname)}")
    else:
        print(f"Archive already extracted : {out_path}")
    return out_path


def extract_bati_path(dir_path) -> str:
    
    target_bati_dir = ["E_BATI", "BATI"]
    target_dir_path=None
    for r, dirs, _ in os.walk(dir_path): 
        if any([_ for _ in dirs if _ in target_bati_dir]):
            dir_name = [_ for _ in dirs if _ in target_bati_dir][0]
            target_dir_path = os.path.join(r, dir_name)
            break  
    if not target_dir_path:
        raise ValueError("please check file paths")
    return target_dir_path


def _extract_bati_new_bdtopo(list_path) -> List[gpd.GeoDataFrame]: 
    """
    for nomenclature >= bdtopo 2023
    """
    
    nature = "Industriel, agricole ou commercial"
    usage = "Industriel"
    usage1b = "Commercial et services"
    print("new nomenclature")
    list_df = []
    
    gen_df = (gpd.read_file(path, crs=CRS) for path in list_path) 
    
    for df in gen_df:
        list_df.append(
            df[
            ((df["NATURE"] == nature) & (df["USAGE1"] == usage)) | 
            ((df["NATURE"] == nature) & (df["USAGE1"] == usage1b) & (df["USAGE2"] == usage))
            ]
        )
    return list_df
 

def _extract_bati_old_bdtopo(list_path: List[str]) -> List[gpd.GeoDataFrame]: 
    list_df = [gpd.read_file(path, crs=CRS) for path in list_path]
    return list_df
    
    
def extract_bati_indus(list_path: List[str], year: str) -> List[gpd.GeoDataFrame]: 
    
    years_new_nomenclature = ["2023"]
    
    if year in years_new_nomenclature:
        return _extract_bati_new_bdtopo(list_path)
    else:
        return _extract_bati_old_bdtopo(list_path)

    

def extract_bati_on_roi(path: str, roi: gpd.GeoDataFrame, year: str) -> gpd.GeoDataFrame:

    
    target_prefix_f = ["BATI_INDUSTRIEL", "BATIMENT"]
    target_suffix_f = ".shp"
    
    target_f_path = [os.path.join(path, f) for f in os.listdir(path) if (Path(f).stem in target_prefix_f) and f.lower().endswith(target_suffix_f)]
    
    list_df = extract_bati_indus(target_f_path, year)
    
    list_df = [gpd.sjoin(df, roi, how="inner", predicate="within") for df in list_df]
    df_y = pd.concat(list_df)
    
    return df_y

def extract_communes_path(dir: str, ext: str) -> str: 
    target_dir_path = None
    for r, dirs, files in os.walk(dir): 
        if any([_ for _ in files if _ == f"COMMUNE{ext}"]):
            target_dir_path = os.path.join(r, f"COMMUNE{ext}")
            break
    return target_dir_path

def extract_communes_on_roi(path: str, roi: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    extracts communes which intersects roi buffer
    """
    df = gpd.read_file(path, crs=CRS)
    df = gpd.sjoin(df, roi, how="inner", predicate="intersects").drop("index_right", axis=1)
    return df



def pipeline_bdtopo_year(dept_list: List[str],
                    name_roi: str,
                    year: str, 
                    centroid: tuple,
                    format="SHP", 
                    clean_dir=True) -> None:
    
    """download industrial building from bdtopo

    Args:
        dept_list (List[str]): list of department codes
        name_roi (str): name of study area (town)
        year (str): list years
        centroid (tuple): point of interest
        format (str, optional): files format for bdtopo. Defaults to "SHP".
        clean_dir (bool, optional): clean bdtopo full directories. Defaults to True.
    """

    out_dir_raw = check_dir(raw_data_path, name_roi, year, "BDTOPO")
    out_dir_processed = check_dir(bati_indus_roi_dir.format(name_roi, year))

    # workaround for 2023 new nomenclature - flr
    ext_file = ".SHP" if year !="2023" else ".shp"

    # define roi
    roi = gpd.GeoDataFrame(geometry=[Point(centroid)], crs=CRS).buffer(DIST_RADIUS).to_frame()

    paths_bati = []
    paths_communes = []
    
    for dept in dept_list:

        # download bdtopo
        arch_path = download_bdtopo(out_dir_raw, dept, year, URL_BDTOPO, format=format)
        bd_path = extract_7z(arch_path, out_dir_raw)
        print(f"work on {bd_path}")
        # extract building on roi
        paths_bati.append(extract_bati_path(bd_path))
        # extract communes on roi
        paths_communes.append(extract_communes_path(bd_path, ext_file))

    print("-- Process buildings and communes --")

    # extract building and communes
    communes = [extract_communes_on_roi(path, roi) for path in paths_communes]
    communes = pd.concat(communes)

    # define unary_union roi based on communes
    rio_com_union = gpd.sjoin(communes, roi, how="inner", predicate="intersects").drop("index_right", axis=1).dissolve()
    
    # extract building on communes
    bati =[extract_bati_on_roi(path, rio_com_union, year) for path in paths_bati]
    bati = pd.concat(bati)

    # Save
    bati.to_parquet(os.path.join(out_dir_processed, bati_indus_file_name.format(name_roi, year)))
    communes.to_parquet(os.path.join(out_dir_processed, communes_roi_file_name.format(name_roi, year)))

    print(f"year {year} done")
    if clean_dir: 
        shutil.rmtree(bd_path)


if __name__ == "__main__":
    metro = '_'.join(METRO_NAME.lower().split(" "))

    for year in selected_year:
        print(f"==== {year} ====")
        pipeline_bdtopo_year(dept_list=dept_list,
                            name_roi=metro,
                            year=year, 
                            centroid=CENTER,
                            format="SHP", 
                            clean_dir=False)

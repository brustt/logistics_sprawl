# Importations de packages
import numpy as np
import geopandas as gpd
import folium
import pandas as pd
from src.config import *
from src.utils import make_path, check_dir
from src.traitements import AppariementRunner
from src.traitements import get_communes_from_radius
from shapely import Point
from typing import Tuple


def compute_statistics(wh_t0, wh_t1, communes_t0, communes_t1, name, period: Tuple[str]):

    period = list(map(int, period))
    global_stats = global_statistics(name, period)
    # get the last date for area | maybe compute for both is better
    area_stats = area_statistics(communes_t1)
    evolution_stats = evoluton_statistics(wh_t0, wh_t1, communes_t0, communes_t1, period)
    tot_stats = {**global_stats, **area_stats, **evolution_stats}

    return tot_stats
    

    

def temporal_based_statistics(wh_df, 
                       communes, 
                       suffix): 

    pop = communes["POPUL"].sum()
    area = communes.unary_union.area
    n_wh = wh_df.ID.nunique()
    unitpop = 1e6

    wh_centroid = np.mean(wh_df.centroid.x), np.mean(wh_df.centroid.y)
    
    stats = {
        f"population_{suffix}": np.round(pop/unitpop, 2),
        f"density_pop_km2_{suffix}": np.round(pop / (area / 1e6), 2),
        f"number_ware_{suffix}": n_wh,
        f"number_ware_per_popM_{suffix}": np.round(n_wh / np.round(pop/unitpop, 2)), 
        f"number_ware_per_1000km2_{suffix}":n_wh / (area / 1000),
        f"avg_size_ware_{suffix}": np.round(wh_df.geometry.area.mean(), 2), 
        f"gravity_{suffix}":  np.round(np.mean(wh_df.distance(Point(wh_centroid))) / 1000, 2)
    }
    
    return stats

def evoluton_statistics(wh_t0, wh_t1, communes_t0, communes_t1, period=Tuple[int]): 

    stats_t0 = temporal_based_statistics(wh_t0, communes_t0, suffix="t0")
    stats_t1 = temporal_based_statistics(wh_t1, communes_t1, suffix="t1")

    stats = {
        "pop_change": (stats_t1["population_t1"] - stats_t0["population_t0"]), 
        "gravity_change": (stats_t1["gravity_t1"] - stats_t0["gravity_t0"]),
        "number_ware_change": (stats_t1["number_ware_t1"] - stats_t0["number_ware_t0"]),
        "perc_ware_change": ((stats_t1["number_ware_t1"] - stats_t0["number_ware_t0"]) / stats_t0["number_ware_t0"])*100,
        "number_ware_per_popM_change": (stats_t1["number_ware_per_popM_t1"] - stats_t0["number_ware_per_popM_t0"]),
        "log_sprawl_measure": (stats_t1["gravity_t1"] - stats_t0["gravity_t0"]) / (period[1] - period[0]),
    }

    stats = {**stats_t0, **stats_t1,**stats}    

    return stats

def global_statistics(name, period=Tuple[int]):

    
    stats = {
        "metro":name,
        "mega_region":None, 
        "country":"France", 
        "continent": "Europe", 
        "data_sources": ','.join(["BDTOPO_IGN", "SIREN"]), 
        "time_period_start": period[0], 
        "time_period_end": period[1],
        "years_data": (period[1] - period[0]),
        "surfaces_area_available":True ,# suppose
        "urban_centrality":None, 
        "gateway": None,
    }

    return stats

def area_statistics(communes): 

    stats = {
        "area": np.round(communes.unary_union.area / 1e6, 1),
        "number_mun":communes.ID.nunique(),
    }
    
    return stats



if __name__ == "__main__": 
    pass
"""
- run analysis for town
- run analysis with different buffer radius
"""
import itertools
import os
from typing import Tuple 
import geopandas as gpd
import pandas as pd
from src.config import *
from src.stats import compute_statistics
from src.traitements import AppariementRunner, get_communes_from_radius
from src.utils import *
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s ::  %(message)s', level = logging.INFO)
logger = logging.getLogger(__name__)

def main(roi_name):
    
    # workaround
    date_list = ['-'.join([_, "01-01"]) for _ in SELECTED_YEARS]
    
    centroid = ENTRY_ROI[roi_name]["CENTER"]

    epochs = list(itertools.combinations(date_list, 2))
    
    logger.info(f"===== Start analysis for {roi_name} ========")
    
    log_sprawl_path = []
    
    for date_start, date_end in epochs:
        logger.info(f"---- Analysis over {get_year_from_datestring(date_start)}-{get_year_from_datestring(date_end)} ----")

        log_sprawl_path.append(logistic_sprawl_analysis(centroid, 
                                                       roi_name, 
                                                       date_start, 
                                                       date_end))
            
    logger.info("=== EPOCHS LOG SPRAWL DONE ====")
    #logger.info(log_sprawl_path)
    return log_sprawl_path



def save_results(df, fname, roi_name):
    out_dir = check_dir(project_path, "reports", roi_name)
    df.to_csv(make_path(fname, out_dir))
    return make_path(fname, out_dir)
    
    
def logistic_sprawl_analysis(centroid: Tuple[float], 
                            roi_name: str, 
                            date_start: str, 
                            date_end: str) -> pd.DataFrame:
    
    log_sprawl_yr = []
    year_start, year_end = get_year_from_datestring(date_start), get_year_from_datestring(date_end)
    
    wh_builder_t0 = AppariementRunner(
        date_analysis=date_start,
        centroid=centroid,
        roi_name=roi_name)

    wh_builder_t1 = AppariementRunner(
        date_analysis=date_end,
        centroid=centroid,
        roi_name=roi_name)
        
    for r in RADIUS_LIST:
            
        #logger.info(f"-- {date_start[:4]} {int(r/1000)}km --")
        
        warehouses_t0 = wh_builder_t0.run(radius=r)
        warehouses_t1 = wh_builder_t1.run(radius=r)


        print(f"WH {year_start} : {warehouses_t0.shape}")
        print(f"WH {year_end} : {warehouses_t1.shape}")


        communes_t0 = get_communes_from_radius(centroid, r, roi_name, year_start, columns=True)
        communes_t1 = get_communes_from_radius(centroid, r, roi_name, year_end, columns=True)


        
        result = compute_statistics(wh_t0=warehouses_t0, 
                    wh_t1=warehouses_t1, 
                    communes_t0=communes_t0, 
                    communes_t1=communes_t1, 
                    name=roi_name, 
                    period=(year_start, year_end))

        df = pd.DataFrame(result, index=[int(r/1000)])
        df.index = df.index.rename("radius")
        
        log_sprawl_yr.append(df)
        
    log_sprawl_yr = pd.concat(log_sprawl_yr, axis=0)
    
    save_results(log_sprawl_yr,
                 f"statistics_{roi_name}_{year_start}_{year_end}.csv",
                 roi_name)    

        
    return log_sprawl_yr
    
    
if __name__ == "__main__":
    roi_name = "lyon"
    main(roi_name)


    
    


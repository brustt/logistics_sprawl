"""
Script qui contient les focntions de traitement des fichiers SIREN et GeoSIREN

Auteurs: C.Colliard, M.Dizier, T.Hillairet
Creation: février 2024
"""

# Importations
from typing import Tuple
import pandas as pd
import geopandas as gpd
import os
from datetime import datetime
from shapely import Point, LineString, Polygon
from shapely.ops import nearest_points
from src.config import *
from src.utils import check_dir, get_year_from_datestring, make_path, timeit
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s ::  %(message)s', level = logging.INFO)
logger = logging.getLogger(__name__)

# Etape 1 : traitement de SIREN
@timeit
def TraitementSiren(date):
    """ Etape 1 : traitement de SIREN

    Args:
        date (string): date de l'étude YYYY-MM-DD.

    Returns:
        pandas.DataFrame: tableau des entrepôts à la date voulue.
    """
    
    # Chemins vers les dossiers de sortie du notebook
    SirenEntrepotsFolder = check_dir(processed_data_path, "SIREN")
    datestr = str(date)

    if not os.path.exists(make_path(siren_name.format(datestr), SirenEntrepotsFolder)):
        
        logger.info("Process Siren file")
        
        # Paramètre de date (string au format 'YYYY-MM-DD')

        date = datetime.strptime(datestr, "%Y-%m-%d")

        # Ouverture du csv SIREN
        siren = pd.read_csv(SirenFPath, 
                            usecols=["siret","activitePrincipaleEtablissement","nomenclatureActivitePrincipaleEtablissement", "dateDebut", "dateFin"], 
                            dtype=str, 
                            )

        # Entités où la nomenclature est renseignée
        siren_nom = siren.dropna(subset=["nomenclatureActivitePrincipaleEtablissement"])
        siren_nap = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAP")]
        siren_naf = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAF1993")]
        siren_na1 = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAFRev1")]
        siren_na2 = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAFRev2")]

        # Entités correspondant à des entrepôts pour chaque nomenclatures
        # Nomenclature NAP
        siren_ent_nap_07 = siren_nap[siren_nap["activitePrincipaleEtablissement"].str.startswith("73.07")]
        siren_ent_nap_08 = siren_nap[siren_nap["activitePrincipaleEtablissement"].str.startswith("73.08")]
        siren_ent_nap = pd.concat([siren_ent_nap_07,siren_ent_nap_08],ignore_index=True)

        # Nomenclature NAF 1993
        siren_ent_naf_D = siren_naf[siren_naf["activitePrincipaleEtablissement"].str.startswith("63.1D")]
        siren_ent_naf_E = siren_naf[siren_naf["activitePrincipaleEtablissement"].str.startswith("63.1E")]
        siren_ent_naf = pd.concat([siren_ent_naf_D,siren_ent_naf_E],ignore_index=True)

        # Nomenclature NAF rev1
        siren_ent_na1_D = siren_na1[siren_na1["activitePrincipaleEtablissement"].str.startswith("63.1D")]
        siren_ent_na1_E = siren_na1[siren_na1["activitePrincipaleEtablissement"].str.startswith("63.1E")]
        siren_ent_na1 = pd.concat([siren_ent_na1_D,siren_ent_na1_E],ignore_index=True)

        # Nomenclature NAF rev2
        siren_ent_na2 = siren_na2[siren_na2["activitePrincipaleEtablissement"].str.startswith("52.1")]

        # Contruction du panda des entrepots selon la date voulue
        siren_ent = pd.concat([siren_ent_nap,siren_ent_naf,siren_ent_na1,siren_ent_na2],ignore_index=True)
        siren_ent["dateFin"] = pd.to_datetime(siren_ent["dateFin"], errors='coerce', format="%Y-%m-%d")
        siren_ent["dateFin"] = siren_ent["dateFin"].fillna(datetime.strptime('2050-01-01','%Y-%m-%d'))
        siren_ent["dateDebut"] = pd.to_datetime(siren_ent["dateDebut"], errors='coerce', format="%Y-%m-%d")
        siren_ent["dateDebut"] = siren_ent["dateDebut"].fillna(datetime.strptime('1900-01-01','%Y-%m-%d'))
        siren_ent = siren_ent[siren_ent["dateDebut"] < date]
        siren_ent = siren_ent[siren_ent["dateFin"] > date]
        
        logger.info(f"SIREN : {siren_ent.shape}")

        # Enregistrement de SIREN entrepôts
        siren_ent.to_csv(make_path(siren_name.format(datestr), SirenEntrepotsFolder), index=False)
        
        del siren_ent
        
        return make_path(siren_name.format(datestr), SirenEntrepotsFolder)

    logger.info("Load Siren file")

    return make_path(siren_name.format(datestr), SirenEntrepotsFolder)

# Etape 2 : traitement de GeoSIREN
@timeit
def TraitementGeoSiren(centroid, 
                       name, 
                       year,
                       r: int = None):
    
    """ Etape 2 : traitement de GeoSIREN

    Args:
        centroid (float): Coordonnée X, Y du centre de la zone d'étude
        r (float): Rayon de la zone d'étude en mètre
        name (string): Nom de la zone d'étude.
        year (string): Année de l'étude.

    Returns:
        pandas.DataFrame: tableau des entités dans la zone d'étude
    """
    
    def load_raw():
        geosiren = pd.read_csv(GeosirenFPath, 
                               sep=';', 
                               usecols=["siret", "x", "y", "epsg"], 
                               dtype={"siret":str}, 
                               )
        # Récupère les données référencées en Lambert 93 (EPSG:2154)
        geosiren = geosiren.loc[geosiren["epsg"] == CRS, :]
        return geosiren
    
    def load_precompute(geosiren_zone_path: str):
        return gpd.read_file(geosiren_zone_path)
        
    precompute=True

    if r is None: #and not os.path.exists(make_path(siren_file_name,  out_dir_siren)): 
        r=DIST_RADIUS
        precompute=False
    
    # check root_dir exist
    root_out_dir = check_dir(processed_data_path, name, year)
    radius_name = int(r/1000)
    
    # already done in download_bdtopo
    ze_dir = check_dir(root_out_dir, "ZoneEtude")
    ze_file_name = ze_name.format(name, radius_name)
    
    out_dir_siren = check_dir(root_out_dir, "SIREN")
    siren_file_name = geosiren_name.format(name, radius_name)
    

    
    if not os.path.exists(make_path(siren_file_name,  out_dir_siren)):
        logger.info(f"Process GeoSiren file {year} - {radius_name}km")
        logger.info("Load GeoSiren file...")

        if not precompute:
            # Ouverture du csv GEOSIREN see config - specify str to siret to prevent error for 0XXX codes (not int)
            geosiren = load_raw()
            logger.info(f"GeoSiren file loaded {year}!")
        else:
            # load pre-compute geosiren for DIST_RADIUS
            geosiren = load_precompute(make_path(geosiren_name.format(name, int(DIST_RADIUS/1000)),  out_dir_siren))
            logger.info(f"FIX GeoSiren file precomputed loaded for {r} - geosiren : {geosiren.shape}!")

        
        # very fast not needed but ok
        if not os.path.exists(make_path(ze_file_name, ze_dir)):
            logger.info("Communes on buffer...")

            ze = get_ze_from_radius(centroid, r, name, year)
            ze.to_file(make_path(ze_file_name, ze_dir))
        
        else :
            logger.info("Load communes on buffer...")
            ze = gpd.read_file(make_path(ze_file_name, ze_dir))

        geosiren = gpd.GeoDataFrame(
            geosiren, geometry=gpd.points_from_xy(x=geosiren.x, y=geosiren.y), crs=CRS
        )
        logger.info("Join geosiren on roi..")

        # better than old way => 25s for 33M rows
        geosiren = (
            gpd.sjoin(
                geosiren, 
                ze, 
                predicate="within", 
                how="inner"
                )
            .drop(["index_right"], axis=1)
        )
        logger.info(f"FIX JOIN GeoSiren  for {year} - geosiren : {geosiren.shape}!")

        logger.info(f"Save geosiren on roi..")

        # Enregistre le GeoSiren de la zone d'étude
        out_dir_siren = check_dir(root_out_dir, "SIREN")
        geosiren.to_file(make_path(siren_file_name,  out_dir_siren), index=False)

        del geosiren
        
        return make_path(siren_file_name,  out_dir_siren)
    
    logger.info("Load GeoSiren file")

    return make_path(siren_file_name,  out_dir_siren)

def get_communes_from_radius(centroid, r, name, year, columns=None):
    # Création du buffer
    buffer = gpd.GeoDataFrame(geometry=[Point(centroid)], crs=CRS).buffer(r).to_frame()
    #buffer.to_file(make_path(ze_file_name, ze_dir))

    # Création de la zone d'étude avec les communes qui intersectes le buffer
    # load from download_topo output : warning communes limit max 25k (default value)
    communes_path = os.path.join(
        communes_roi_dir.format(name, year),
        communes_roi_file_name.format(name, year)
    )
    communes = gpd.read_file(communes_path).to_crs(CRS)
    
    columns = list(communes.columns) if columns else ["geometry"]    
        
    ze = (
        gpd.sjoin(
            communes[columns], 
            buffer, 
            predicate="intersects", 
            how="inner"
            )
        .drop("index_right", axis=1)
    )
    
    ze = ze.rename({"POPULATION":"POPUL"}, axis=1)
    
    return ze

def get_ze_from_radius(centroid, r, name, year, columns=None):
    
    # Création du buffer
    buffer = gpd.GeoDataFrame(geometry=[Point(centroid)], crs=CRS).buffer(r).to_frame()
    #buffer.to_file(make_path(ze_file_name, ze_dir))

    # Création de la zone d'étude avec les communes qui intersectes le buffer
    # load from download_topo output : warning communes limit max 25k (default value)
    communes_path = os.path.join(
        communes_roi_dir.format(name, year),
        communes_roi_file_name.format(name, year)
    )
    communes = gpd.read_file(communes_path).to_crs(CRS)
    
    columns = list(communes.columns) if columns else ["geometry"]    
        
    ze = (
        gpd.sjoin(
            communes[columns], 
            buffer, 
            predicate="intersects", 
            how="inner"
            )
        .drop("index_right", axis=1)
        .dissolve()
    )
    
    ze = ze.rename({"POPULATION":"POPUL"}, axis=1)    
    return ze

# Etape 3 : jointure SIREN et GeoSIREN
@timeit
def JoinSirenGeosiren(siren_date_path: str, 
                      geosiren_zone_path: str, 
                      year: str, 
                      name: str, 
                      r: int):
    """ Etape 3 : jointure SIREN et GeoSIREN.

    Args:
        siren_date (pandas.DataFrame): tableau des entrepôts dans SIREN à la date voulue (étape 1).
        geosiren_zone (pandas.DataFrame): tableau des bâtiments dans le buffer_max
        year (string): annee de l'étude YYYY
        name (string): nom de la zone d'étude.
        r (float): rayon de la zone d'étude en km.

    Returns:
        pandas.DataFrame: tableau des entrepôts dans la zone d'étude à la date voulue.
    """
    logger.info("Merge Siren and GeoSiren")
    # check root_dir exist
    radius_name= int(r/1000)
    root_out_dir = check_dir(processed_data_path, name, year, "Entrepots")
    wh_file_name = warehouse_name.format(name, year, radius_name)
    
    if not os.path.exists(make_path(wh_file_name, root_out_dir)):
        
        siren_date = pd.read_csv(siren_date_path)
        geosiren_zone = gpd.read_file(geosiren_zone_path)
        
        logger.info(f"Siren {siren_date.shape}")
        logger.info(f"Geosiren {geosiren_zone.shape}")


        # Jointure sur le champ SIRET - we convert to int  - doesn't matter
        siren_date["siret"] = siren_date["siret"].astype(int)
        geosiren_zone["siret"] = geosiren_zone["siret"].astype(int)

        merged_siren = pd.merge(siren_date, geosiren_zone, on="siret")
        merged_siren = gpd.GeoDataFrame(merged_siren, geometry="geometry", crs=CRS)
        # Enregistre la jointure
        merged_siren.to_file(make_path(wh_file_name, root_out_dir), index=False)
        logger.info(f"MERGE SIREN : {merged_siren.shape}")

        del siren_date
        del geosiren_zone
        del merged_siren
        
        return make_path(wh_file_name, root_out_dir)

    logger.info("Load merge Siren and GeoSiren")

    return make_path(wh_file_name, root_out_dir)

# Etape 4 : Appariement SIREN entrepôts et BDTOPO bâti industriel
@timeit
def AppSirenBDTopo(name: str,
                   entrepots_siren_path: str,
                   year: str,
                   r: int,
                   dist_siren_bdtopo: float=50.0,
                   seuil_surf_ent: float=1000.0):
    
    """ Etape 4 : Appariement SIREN entrepôts et BDTOPO bâti industriel.

    Args:
        name : (str) : roi name
        entrepots_siren (pandas.DataFrame): tableau des entrepôts de SIREN dans la zone d'étude.
        year (string): année de l'étude.
        zone (string): nom de la zone d'étude.
        r (float): rayon de la zone d'étude en km.
        dist_siren_bdtopo (float, optional): distance maximale entre SIREN et BDTOPO. Defaults to 50.0.
        seuil_surf_ent (float, optional): surface minimale d'un entrepot. Defaults to 1000.0.

    Returns:
        geopandas.GeoDataFrame: tableau des bâtiments d'entrepôt dans la zone d'étude.
    """
    logger.info("Appariement processing...")
    
    # check root_dir exist
    root_out_dir = check_dir(processed_data_path, name, year)
    app_our_dir = check_dir(root_out_dir, "Appariement")
    
    appariement_file_name = appariement_name.format(name.upper(), year, int(r/1000))
    
    #ze_file_name = ze_name.format(name, int(r/1000))
    #ze_dir = make_path(ze_file_name, root_out_dir, "ZoneEtude")
    
    if not os.path.exists(make_path(appariement_file_name, root_out_dir)):
        
        entrepots_siren = gpd.read_file(entrepots_siren_path)
        logger.info(f"Entrepot merge SIREN  {year}: {entrepots_siren.shape}")
        
        # Ouvertur du shapefile des bâtiments industriels de la bdtopo
        bati_indus = (
            gpd.read_file(
                make_path(
                    bati_indus_file_name.format(name, year),
                    bati_indus_roi_dir.format(name, year)
                    )
                ).reset_index(drop=True)
        )
        logger.info(f"Batis Indus BDTOPO : {bati_indus.shape}")

        # Calcul des lignes la plus courtes entre un point d'entrepot siren et un batiment industriel de la bd topo (point à bord)
        lines,id = [],0
        for idx, point in entrepots_siren.iterrows():
            nearest_bat = bati_indus.geometry.distance(point.geometry).idxmin()
            bat_geom = bati_indus.loc[nearest_bat,'geometry']
            nearest_point_bat = nearest_points(point.geometry,bat_geom)[1]
            nearest_point_bat = Point(nearest_point_bat.x,nearest_point_bat.y)
            line = LineString([point.geometry,nearest_point_bat])
            item = ({'id':id, 'geometry':line})
            lines.append(item)
            id += 1
        print(len(lines))
        lines_gdf = gpd.GeoDataFrame(lines, geometry='geometry',crs=CRS)

        # Calcul des lignes qui sont plus grande que la distance max voulue avec un batiment de la bdtopo
        lines_app = lines_gdf[lines_gdf["geometry"].length < dist_siren_bdtopo]
        logger.info(f"Lines dist_min : {lines_app.shape}")

        # Calcul des bâtiment de la bdtopo correspondant à un point d'entrepôt siren et dont la surface est plus grande que le seuil voulu
        bati_indus_ent = bati_indus[bati_indus["geometry"].intersects(lines_app.unary_union) | bati_indus["geometry"].intersects(entrepots_siren.unary_union)]
        bati_indus_ent = bati_indus_ent[bati_indus_ent["geometry"].area > seuil_surf_ent]
        
        # Enregistrement des entrepôts à la date voulue
        
        bati_indus_ent = bati_indus_ent.drop_duplicates(keep="first")

        bati_indus_ent.to_file(make_path(appariement_file_name, app_our_dir))
        
        logger.info(f"Appariement done for {name} on {year} : {bati_indus_ent.shape}")
        return bati_indus_ent

    logger.info("Load appariement")

    return gpd.read_file(make_path(appariement_file_name, root_out_dir))


class AppariementRunner:
    def __init__(self, 
                 date_analysis: str,
                 centroid:Tuple[float],
                 roi_name:str):
        
        self.centroid = centroid
        self.roi_name = '_'.join(roi_name.lower().split(" "))
        self.date_analysis = date_analysis
        
        # pre compute data for max buffer
        self.siren_ent_path = TraitementSiren(self.date_analysis)
        self.geosiren_buffer_path = TraitementGeoSiren(centroid=self.centroid,
                                             name=self.roi_name,
                                             year=get_year_from_datestring(self.date_analysis),
                                             r=None
                                             )
        
    @timeit
    def run(self, radius:int):
        

        self.geosiren_buffer_path = TraitementGeoSiren(centroid=self.centroid,
                                             name=self.roi_name,
                                             year=get_year_from_datestring(self.date_analysis),
                                             r=radius
                                             )
        
        logger.info(f"Geosiren : {self.geosiren_buffer_path}")
        
        merged_siren_path = JoinSirenGeosiren(siren_date_path=self.siren_ent_path,
                                        geosiren_zone_path=self.geosiren_buffer_path,
                                        year=get_year_from_datestring(self.date_analysis),
                                        name=self.roi_name,
                                        r=radius)

        
        logger.info(f"Merge : {merged_siren_path}")

        warehouses = AppSirenBDTopo(name=self.roi_name,
                                    entrepots_siren_path=merged_siren_path,
                                    year=get_year_from_datestring(self.date_analysis),
                                    r=radius)
        
        return warehouses
    
    
if __name__ == "__main__":
    
    roi_name = ROI_NAME[:]
    
    # workaround
    date_list = ['-'.join([_, "01-01"]) for _ in SELECTED_YEARS]

    # Paramétrage de la période d'étude    
    for date_start in date_list:
    
        logger.info(f"== {date_start} == ")
        
        
        wh_builder = AppariementRunner(
            date_analysis=date_start,
            centroid=ENTRY_ROI[roi_name]["CENTER"],
            roi_name=roi_name)
        
        for r in RADIUS_LIST:
            
            logger.info(f"-- {date_start[:4]} {int(r/1000)}km --")
        
            wh = wh_builder.run(radius=r)
        
            print(wh.shape)
    
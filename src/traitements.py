"""
Script qui contient les focntions de traitement des fichiers SIREN et GeoSIREN

Auteurs: C.Colliard, M.Dizier, T.Hillairet
Creation: février 2024
"""

# Importations
import pandas as pd
import geopandas as gpd
import os
from datetime import datetime
from shapely import Point, LineString, Polygon
from shapely.ops import nearest_points
from src.config import *
from src.utils import check_dir, get_year_from_datestring, make_path
import logging

logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Etape 1 : traitement de SIREN
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
        siren = pd.read_csv(SirenFPath, usecols=["siret","activitePrincipaleEtablissement","nomenclatureActivitePrincipaleEtablissement", "dateDebut", "dateFin"], dtype=str)

        # Entités où la nomenclature est renseignée
        siren_nom = siren.dropna(subset=["nomenclatureActivitePrincipaleEtablissement"])
        siren_nap = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAP")]
        siren_naf = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAF19NAFRev1")]
        siren_na2 = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("NAFRe93")]
        siren_na1 = siren_nom[siren_nom["nomenclatureActivitePrincipaleEtablissement"].str.startswith("v2")]

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

        # Enregistrement de SIREN entrepôts
        siren_ent.to_parquet(make_path(siren_name.format(datestr), SirenEntrepotsFolder), index=False)
        
        return siren_ent

    logger.info("Load Siren file")

    return pd.read_parquet(make_path(siren_name.format(datestr), SirenEntrepotsFolder))

# Etape 2 : traitement de GeoSIREN
def TraitementGeoSiren(x, y, r, name, year):
    """ Etape 2 : traitement de GeoSIREN

    Args:
        x (float): Coordonnée X du centre de la zone d'étude
        y (float): Coordonnée Y du centre de la zone d'étude
        r (float): Rayon de la zone d'étude en mètre
        name (string): Nom de la zone d'étude.
        year (string): Année de l'étude.

    Returns:
        pandas.DataFrame: tableau des entités dans la zone d'étude
    """
    
    # check root_dir exist
    root_out_dir = check_dir(processed_data_path, name, year)
    radius = int(r/1000)
    
    out_dir_siren = check_dir(root_out_dir, "SIREN")
    siren_file_name = geosiren_name.format(name, radius)
    
    if not os.path.exists(make_path(siren_file_name,  out_dir_siren)):
        logger.info("Process GeoSiren file")
        logger.info("Load GeoSiren file...")

        # Ouverture du csv GEOSIREN see config
        geosiren = pd.read_csv(GeosirenFPath, sep=';', usecols=["siret", "x", "y", "epsg"])
        
        logger.info("GeoSiren file loaded !")


        # Récupère les données référencées en Lambert 93 (EPSG:2154)
        geosiren_lamb = geosiren[geosiren["epsg"] == CRS]
        
        # already done in download_bdtopo
        ze_dir = check_dir(root_out_dir, "ZoneEtude")
        ze_file_name = ze_name.format(name, radius)
        if not os.path.exists(make_path(ze_file_name, ze_dir)):

            # Création du buffer
            point = Point((x,y))
            gdf = gpd.GeoDataFrame(geometry=[point], crs=CRS)
            buffer = gdf['geometry'].buffer(r)
            buffer.to_file(make_path(ze_file_name, ze_dir))

            # Création de la zone d'étude avec les communes qui intersectes le buffer
            # load from download_topo output : warning communes limit max 25k (default value)
            communes_path = os.path.join(
                communes_roi_dir.format(name, year),
                communes_roi_file_name.format(name, year)
            )
            communes = gpd.read_parquet(communes_path)
            
            ze = gpd.GeoDataFrame([{
                "id":0,
                "geometry":communes[communes['geometry'].intersects(buffer.unary_union)].unary_union
            }], geometry="geometry", crs=CRS)

            # Enregistre la zone d'étude
            ze.to_file(make_path(ze_file_name, ze_dir))
        
        else :
            ze = gpd.read_parquet(make_path(ze_file_name, ze_dir))

        # Liste des géométries 
        points = [Point(xy) for xy in zip(geosiren_lamb["x"], geosiren_lamb["y"])]

        # Transforme en GeoDataFrame et renseigne EPSG = 2154 
        geosiren_geom = gpd.GeoDataFrame(geosiren_lamb, geometry=points, crs=CRS)

        # Converti en GeoSeries pour utiliser within()
        geometries = gpd.GeoSeries(geosiren_geom["geometry"])
        
        logger.info("Join geosiren on roi..")


        # Récupère les lignes dans la zone d'étude
        geosiren_buffer = geosiren_lamb[geometries.within(ze.unary_union)]
        logger.info("Save geosiren on roi..")

        # Enregistre le GeoSiren de la zone d'étude
        out_dir_siren = check_dir(root_out_dir, "SIREN")
        siren_file_name = geosiren_name.format(name, radius)
        geosiren_buffer.to_parquet(make_path(siren_file_name,  out_dir_siren), index=False)
        
        return geosiren_buffer
    
    logger.info("Load GeoSiren file")

    return pd.read_parquet(make_path(siren_file_name,  out_dir_siren))

# Etape 3 : jointure SIREN et GeoSIREN
def JoinSirenGeosiren(siren_date,geosiren_zone,date,name,r):
    """ Etape 3 : jointure SIREN et GeoSIREN.

    Args:
        siren_date (pandas.DataFrame): tableau des entrepôts dans SIREN à la date voulue (étape 1).
        geosiren_zone (pandas.DataFrame): tableau des bâtiments dans la zone d'étude (étape 2).
        date (string): date de l'étude YYYY-MM-DD.
        name (string): nom de la zone d'étude.
        r (float): rayon de la zone d'étude en km.

    Returns:
        pandas.DataFrame: tableau des entrepôts dans la zone d'étude à la date voulue.
    """
    logger.info("Merge Siren and GeoSiren")
    # check root_dir exist
    year = get_year_from_datestring(date)
    root_out_dir = check_dir(processed_data_path, name, year, "Entrepots")
    wh_file_name = warehouse_name.format(name, date, int(r))
    
    if not os.path.exists(make_path(wh_file_name, root_out_dir)):

        # Conversion de siret en entier
        siren_date["siret"] = siren_date["siret"].astype(int)
        geosiren_zone["siret"] = geosiren_zone["siret"].astype(int)

        # Jointure sur le champ SIRET
        merged_siren = pd.merge(siren_date, geosiren_zone, on="siret")

        # Enregistre la jointure

        merged_siren.to_parquet(make_path(wh_file_name, root_out_dir), index=False)

        return merged_siren
    
    logger.info("Load merge Siren and GeoSiren")

    return pd.read_parquet(make_path(wh_file_name, root_out_dir))

# Etape 4 : Appariement SIREN entrepôts et BDTOPO bâti industriel
def AppSirenBDTopo(name,
                   entrepots_siren,
                   year,
                   zone,
                   r,
                   dist_siren_bdtopo=50.0,
                   seuil_surf_ent=1000.0):
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
    root_out_dir = check_dir(processed_data_path, name, year, 'Appariement')
    
    appariement_file_name = appariement_name.format(zone, year, int(r))
    if not os.path.exists(make_path(appariement_file_name, root_out_dir)):

        # Ouvertur du shapefile des bâtiments industriels de la bdtopo
        bati_indus = (
            gpd.read_parquet(
                make_path(
                    bati_indus_file_name.format(name, year),
                    bati_indus_roi_dir.format(name, year)
                    )
                )
        )
            
        # Transforme en GeoDataFrame et renseigne EPSG = 2154 
        points = [Point(xy) for xy in zip(entrepots_siren["x"], entrepots_siren["y"])]
        entrepots_siren = gpd.GeoDataFrame(entrepots_siren, geometry=points, crs=CRS)
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
        lines_gdf = gpd.GeoDataFrame(lines, geometry='geometry',crs=CRS)

        # Calcul des lignes qui sont plus grande que la distance max voulue avec un batiment de la bdtopo
        lines_app = lines_gdf[lines_gdf["geometry"].length < dist_siren_bdtopo]

        # Calcul des bâtiment de la bdtopo correspondant à un point d'entrepôt siren et dont la surface est plus grande que le seuil voulu
        bati_indus_ent = bati_indus[bati_indus["geometry"].intersects(lines_app.unary_union) | bati_indus["geometry"].intersects(entrepots_siren.unary_union)]
        bati_indus_ent = bati_indus_ent[bati_indus_ent["geometry"].area > seuil_surf_ent]

        # Enregistrement des entrepôts à la date voulue
        
        bati_indus_ent.to_parquet(make_path(appariement_file_name, root_out_dir))
        
        logger.info("Appariement done !")
        return bati_indus_ent

    logger.info("Load appariement")

    return pd.read_parquet(make_path(appariement_file_name, root_out_dir))


class AppariementRunner:
    def __init__(self, 
                 roi_name,
                 date_start: str, 
                 date_end: str, 
                 radius: int, 
                 geosiren_buffer: gpd.GeoDataFrame=None):
        
        self.time_period_start = get_year_from_datestring(date_start)
        self.time_period_end = get_year_from_datestring(date_start)
        
        self.roi_name = '_'.join(roi_name.lower().split(" "))
        self.date_start = date_start
        self.date_end = date_end
        self.radius = int(radius/1000)
        
        if not geosiren_buffer:
            self.geosiren_buffer = TraitementGeoSiren(CENTER[0],
                                             CENTER[1],
                                             self.radius,
                                             self.roi_name,
                                             self.time_period_end)

    def run(self):
        
        siren_ent = TraitementSiren(self.date_start)
        merged_siren = JoinSirenGeosiren(siren_ent,
                                            self.geosiren_buffer,
                                            self.date_start,
                                            self.roi_name,
                                            self.radius)
        
        warehouses = AppSirenBDTopo(self.roi_name,
                                    merged_siren,
                                    self.time_period_start,
                                    self.roi_name,
                                    self.radius)

        return warehouses
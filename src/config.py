import os
from dotenv import find_dotenv


CRS = 2154


project_path = os.path.dirname(find_dotenv())

data_path = os.path.join(project_path, "data/")
raw_data_path = os.path.join(project_path, "data/raw")
processed_data_path = os.path.join(project_path, "data/processed")

"""bd topo parameters"""
URL_BDTOPO = "https://geoservices.ign.fr/bdtopo"
selected_year = ["2008", "2013" ,"2023"] 
dept_list = ["01", "38", "69"]

# Chemins vers le dossier des fichiers BDTOPO communes
BDTopoFPath = os.path.join("BDTOPO/COMMUNES","{}", "COMMUNES_{}.gpkg")

# chemins bati indus & communes for roi and year
bati_indus_roi_dir = os.path.join(processed_data_path, "BDTOPO", "{}", "{}") #name, year, file name
bati_indus_file_name = "bati_indus_{}_{}.shp"

communes_roi_dir = os.path.join(processed_data_path, "BDTOPO", "{}", "{}") #name, year
communes_roi_file_name = "communes_{}.shp"



"""roi parameters"""
# Paramétrage de la zone d'étude
METRO_NAME = "LYON" # nom de la zone en string
CENTER = (841650.0, 6517765.0) # coordonnées du centre de la zone d'étude (x,y)
DIST_RADIUS = 25_000


"""SIREN"""

# Chemins vers le dossier des fichiers SIREN et leur nom
GeosirenFPath = os.path.join(raw_data_path, "SIREN", "GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv")

SirenFPath = os.path.join(raw_data_path, "SIREN", "StockEtablissementHistorique_utf8.csv")



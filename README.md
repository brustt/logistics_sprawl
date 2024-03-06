# logistics_sprawl

IGAST Spatial Analysis project

* C. Colliard
* T. Hillairet
* M. Dizier

---
Analysis of logistics sprawl over French cities. POC over Lyon.


python=3.10

---
Installation : 

* Clone this repo

* Create virtual env (mamba, virtualenv whetever you like) and activate it.

* create `.env` file at the project directory level

* Install dependencies : 
```
pip install -r requirements.txt
```

* Install src as package : 
```
pip install -e .
```

create `data` directory

download SIREN data from https://drive.google.com/drive/folders/1y2DO8XNg6cGofzZwt_kVRMnDGZs4SE82 in `data/raw`

* Define region of interest in config.py (centroid, buffer size and name)
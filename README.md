# Analysis of logistics sprawl over French cities

IGAST Spatial Analysis project 

* C. Colliard
* T. Hillairet
* M. Dizier

Sponsor : L.Dablanc (Logistic Chair, Université Gustave Effel)
---

Dablanc, 2023
https://www.lvmt.fr/wp-content/uploads/2019/10/Dablanc-Schorung-De-Oliveira-Palacios-Arguello-De-Oliveira-Yaghi-October-2023.pdf


Compute statistics for logistic sprawl with new automatic method generalizable over french metropolitan cities.

Towns of study : Lyon (new in Logistic Chair BDD) and Bordeaux (use for comparison)

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
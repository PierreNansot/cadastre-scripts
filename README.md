# Cadastre français : de Geojson à Postgis  

Script d'import  fichiers geojson cadastre vers base de donnée Postgis

## Techno

- Python 3.5
- Ogr2Ogr version 2.*

## Attention !!

- Retry pour les téléchargements des listes et des fichiers
- Multithreading **x12** (faire attention sur petit pc)
- ogr2ogr traite 25000 entités par requête, si une seule entité déconne, c'est les 25000 qui saute

## Résultat pour parcelles

Environ 87 millions de lignes. Table de 30go.


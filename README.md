# Base nationale des décès

Ce dépôt accueille le script SQL de génération d'une base nationale des décès (source Insee) affinée, avec les retraitements suivants :

- Suppression de doublons (environ 220 000),
- Recodage simplifié du pays de naissance,
- Création de vrais champs temporels de type date, permettant de calculer un âge au décès. Quand le jour manque, il est conventionnellement fixé à 15 ; quand le mois manque, le milieu de l’année est retenu ; création associée d’une variable indicatrice du caractère de flou des dates ;
- Base triée pour un format parquet de compression optimisée.

Elle est disponible sur [data.gouv.fr](https://www.data.gouv.fr/datasets/base-nationale-des-deces-dedoublonnee)

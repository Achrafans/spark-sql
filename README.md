# TP Spark SQL - Analyse d'incidents

Ce projet Java utilise **Apache Spark SQL** pour analyser un fichier CSV contenant des incidents signalés par différents services d'une entreprise industrielle.

## Structure du fichier CSV

Le fichier `incidents.csv` doit être au format suivant :

id : identifiant de l'incident

titre : titre court

description : détail de l’incident

service : service concerné

date : au format yyyy-MM-dd

# Objectifs du TP
Afficher le nombre d’incidents par service

Afficher les deux années ayant eu le plus d’incidents

# Exécution du projet
# Prérequis
Java 17 ou Java 8

Maven

Apache Spark 3.5.x (géré via dépendances Maven)

# Compilation
bash
Copier
Modifier
mvn clean install
 

# Exemple de sortie 
Nombre d'incidents par service :
+------------+-----+
|     service|count|
+------------+-----+
|Informatique|    4|
| Maintenance|    2|
|  Production|    2|
|    Sécurité|    1|
|     Énergie|    1|
+------------+-----+

Les deux années avec le plus d'incidents :
+-----+-----+
|annee|count|
+-----+-----+
| 2023|    4|
| 2022|    4|
+-----+-----+
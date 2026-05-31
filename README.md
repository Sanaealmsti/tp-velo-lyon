# TP Data Lake Velo Lyon

Sujet : Projet de data lake pour les stations Velo'v de Lyon.
On recupère les données en temps réel depuis l'API JCDecaux, on les stocke dans HDFS, on les transforme avec MapReduce, on les interroge avec Hive et on trace leur origine avec Atlas.

## Comment lancer le projet

Il faut Docker et Python 3 installés puis :

```bash
#Lancer le cluster
docker compose up -d --build
sleep 40

#Créer les dossiers dans hdfs
docker compose exec namenode hdfs dfs -mkdir -p /data-lake/raw/velo_lyon
docker compose exec namenode hdfs dfs -mkdir -p /data-lake/processed/load_metrics
docker compose exec namenode hdfs dfs -mkdir -p /data-lake/processed/anomalies
docker compose exec namenode hdfs dfs -chmod -R 777 /data-lake
```

## Recupèrer les données

Il faut créer un fichier .env avec sa clé API JCDecaux (https://developer.jcdecaux.com) :

```
JCDECAUX_API_KEY=...
KAFKA_BOOTSTRAP=localhost:29092
```

Puis dans deux terminaux :

```bash
# Terminal 1
source .venv/bin/activate
python kafka/recup_api_jcdecaux.py

# Terminal 2
source .venv/bin/activate
python kafka/ecriture_hdfs.py
```

Laisser tourner quelques minutes puis quitter

## Lancer les jobs MapReduce

```bash
# MR1 load factor
docker compose exec resourcemanager bash -c "mapred streaming -input /data-lake/raw/velo_lyon/* -output /data-lake/processed/load_metrics -mapper 'python3 mapper_load_factor.py' -reducer 'python3 reducer_load_factor.py' -file /opt/project/mapper_load_factor.py -file /opt/project/reducer_load_factor.py"

# MR2 anomalies
docker compose exec resourcemanager bash -c "mapred streaming -input /data-lake/raw/velo_lyon/* -output /data-lake/processed/anomalies -mapper 'python3 mapper_anomalies.py' -reducer 'python3 reducer_anomalies.py' -file /opt/project/mapper_anomalies.py -file /opt/project/reducer_anomalies.py"
```

## Lancer les requêtes métier Hive

Le fichier create_hive_tables.sql crée 2 tables externes qui pointent vers les donnees dans HDFS.
Le fichier requetes_metier.sql contient 6 requetes qui repondent à des questions métier (stations vides, quartiers en tension, évolution station 2010, stations en panne, taux de remplissage, validité GPS).

```bash
docker compose exec hive beeline -u "jdbc:hive2://localhost:10000/" -f /opt/project/create_hive_tables.sql
docker compose exec hive beeline -u "jdbc:hive2://localhost:10000/" -f /opt/project/requetes_metier.sql
```

## Atlas

Les 3 datasets du data lake sont declarés dans atlas_entities.json (raw_velo_stations, processed_load_factor, processed_anomalies). Pour les envoyer dans Atlas :

```bash
curl -X POST -u admin:admin -H 'content-type: application/json' http://localhost:21000/api/atlas/v2/entity -d @atlas_entities.json
```

Interfaces web

- HDFS : http://localhost:9870
- YARN : http://localhost:8088
- Hive : http://localhost:10002
- Atlas : http://localhost:21000

#Fiabilité et nombre d'anomalies par station
import sys

station_en_cours = None
nb_anomalies = 0
total = 0
derniere_panne = ""
dernier_ts = 0

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne:
        continue
    morceaux = ligne.split("\t")
    if len(morceaux) != 4:
        continue

    station = morceaux[0]
    type_anomalie = morceaux[1]
    try:
        ts = int(morceaux[2])
    except:
        continue

    if station != station_en_cours:
        if station_en_cours is not None:
            if total > 0:
                fiab = int(((total - nb_anomalies) / total) * 100)
            else:
                fiab = 0
            print(station_en_cours + "\t" + str(fiab) + "%" + "\t" + str(nb_anomalies) + "\t" + derniere_panne)
        station_en_cours = station
        nb_anomalies = 0
        total = 0
        derniere_panne = ""
        dernier_ts = 0

    total = total + 1
    nb_anomalies = nb_anomalies + 1
    if ts > dernier_ts:
        dernier_ts = ts
        derniere_panne = type_anomalie

if station_en_cours is not None:
    if total > 0:
        fiab = int(((total - nb_anomalies) / total) * 100)
    else:
        fiab = 0
    print(station_en_cours + "\t" + str(fiab) + "%" + "\t" + str(nb_anomalies) + "\t" + derniere_panne)

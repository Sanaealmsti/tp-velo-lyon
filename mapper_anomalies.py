#detection des anomalies par station
import sys
import json
import time

maintenant = int(time.time())

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne:
        continue
    try:
        d = json.loads(ligne)
    except:
        continue

    num = d.get("number")
    velos = d.get("available_bikes")
    bornes = d.get("available_bike_stands")
    total = d.get("bike_stands")
    statut = d.get("status")
    ts = d.get("last_update")

    if num is None or ts is None:
        continue

    ts_sec = int(ts / 1000)
    age = maintenant - ts_sec

    #Pas de mise a jour depuis 30 minutes
    if age > 1800:
        print(str(num) + "\t" + "NO_UPDATE" + "\t" + str(ts_sec) + "\t" + str(age))

    #Cas de station ouverte mais 0 vélo
    if statut == "OPEN" and velos == 0:
        print(str(num) + "\t" + "ZERO_BIKES" + "\t" + str(ts_sec) + "\t" + str(age))

    #CAs où toutes les bornes sont vides
    if bornes == total and total > 0:
        print(str(num) + "\t" + "FULL_STANDS" + "\t" + str(ts_sec) + "\t" + str(age))

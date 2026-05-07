#cClcul du load factor par station
import sys
import json

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne:
        continue
    try:
        d = json.loads(ligne)
    except:
        continue

    velos = d.get("available_bikes")
    bornes = d.get("available_bike_stands")
    total = d.get("bike_stands")
    statut = d.get("status")
    ts = d.get("last_update")
    num = d.get("number")

    if velos is None or bornes is None or total is None:
        continue
    if velos + bornes == 0:
        continue

    lf = velos / (velos + bornes)

    if statut == "OPEN" and velos >= 0 and velos <= total:
        valide = 1
    else:
        valide = 0

    print(str(num) + "\t" + str(int(ts/1000)) + "\t" + str(round(lf, 3)) + "\t" + str(valide))

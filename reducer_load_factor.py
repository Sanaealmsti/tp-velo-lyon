#Moyenne et écart type du load factor par station
import sys
import math

station_en_cours = None
valeurs = []
total = 0

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne:
        continue
    morceaux = ligne.split("\t")
    if len(morceaux) != 4:
        continue

    station = morceaux[0]
    try:
        lf = float(morceaux[2])
        v = int(morceaux[3])
    except:
        continue

    if station != station_en_cours:
        if station_en_cours is not None:
            if len(valeurs) > 0:
                moy = sum(valeurs) / len(valeurs)
                var = sum((x - moy) ** 2 for x in valeurs) / len(valeurs)
                ecart = math.sqrt(var)
            else:
                moy = 0
                ecart = 0
            print(station_en_cours + "\t" + str(round(moy, 2)) + "\t" + str(round(ecart, 2)) + "\t" + str(len(valeurs)) + "/" + str(total))
        station_en_cours = station
        valeurs = []
        total = 0

    total = total + 1
    if v == 1:
        valeurs.append(lf)

#Dernière station
if station_en_cours is not None:
    if len(valeurs) > 0:
        moy = sum(valeurs) / len(valeurs)
        var = sum((x - moy) ** 2 for x in valeurs) / len(valeurs)
        ecart = math.sqrt(var)
    else:
        moy = 0
        ecart = 0
    print(station_en_cours + "\t" + str(round(moy, 2)) + "\t" + str(round(ecart, 2)) + "\t" + str(len(valeurs)) + "/" + str(total))

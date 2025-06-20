# Convert GeoJson

# Parameters
import json, os, arcpy
from datetime import datetime

# importeer Awv functie en downloadFunctie
from sys import path

basispath = os.path.realpath(__file__).split("awvDataTools")[0]
print("basispath = ", basispath)
path2 = os.path.join(basispath, "awvDataTools", "AwvFuncties")
path.append(path2)
import AwvFuncties

##import downloadGeolatteNoSqlFuncties

reload(AwvFuncties)
try:
    importlib.reload(AwvFuncties)
    ##    importlib.reload(downloadGeolatteNoSqlFuncties)
    arcpy.AddMessage("reload python 3")
except:
    reload(AwvFuncties)
    ##    reload(downloadGeolatteNoSqlFuncties)
    arcpy.AddMessage("reload python 2")


# lees waarden
def leesOpstelling(jsonLine):
    # "[u'OBJECTID', u'SHAPE', u'ident8', u'gebied', u'gekozenbeheerder', u'begindatum', u'actueleOpstelling', u'berekendeBeheerder', u'id']"
    x = jsonLine["geometry"]["coordinates"][0]
    y = jsonLine["geometry"]["coordinates"][1]
    geometryWkt = "POINT (%s %s)" % (x, y)

    jsonLineProperties = jsonLine["properties"]

    veldenLijst = [
        "id",
        "wegsegmentid",
        "straat",
        "ident8",
        "opschrift",
        "afstand",
        "langsGewestweg",
        "zijdeVanDeRijweg",
        "gemeente",
        "actueleOpstelling",
        "beheerder",
        "status",
        "beginDatum",
        "wijzigingsDatum",
        "copyDatum"
    ]

    veldenDict = {}

    for veld in veldenLijst:
        try:
            if veld == "beheerder":
                veldenDict[veld] = jsonLineProperties[veld]["gebiednummernaam"]
            else:
                veldenDict[veld] = jsonLineProperties[veld]
        except:
            veldenDict[veld] = None

    opstellingsFeature = [
        geometryWkt,
        veldenDict["id"],
        veldenDict["wegsegmentid"],
        veldenDict["straat"],
        veldenDict["ident8"],
        veldenDict["opschrift"],
        veldenDict["afstand"],
        veldenDict["langsGewestweg"],
        veldenDict["zijdeVanDeRijweg"],
        veldenDict["gemeente"],
        veldenDict["actueleOpstelling"],
        veldenDict["beheerder"],
        veldenDict["status"],
        veldenDict["beginDatum"],
        veldenDict["wijzigingsDatum"],
        datetime.now().strftime('%d/%m/%Y')
    ]

    jsonLineAanzichtLijst = jsonLineProperties["aanzichten"]

    return opstellingsFeature, veldenDict["id"], geometryWkt, jsonLineAanzichtLijst


def leesAanzicht(opstellingsid, geometryWkt, jsonLineAanzicht):
    from datetime import datetime
    ##    print "***opstellingsid = %s" %opstellingsid
    aanzichtId = jsonLineAanzicht["id"]
    aanzichtClientId = jsonLineAanzicht["clientId"]
    hoek = round(jsonLineAanzicht["hoek"] * 180 / 3.14, 0)
    wegsegmentid=jsonLineAanzicht["wegsegmentid"]
    wegsegmentidGok=jsonLineAanzicht["wegsegmentidGok"]
    wijzigingsTimestamp = (datetime.utcfromtimestamp(jsonLineAanzicht["wijzigingsTimestamp"] / 1000)).strftime(
        '%d/%m/%Y')
    informeert = jsonLineAanzicht["type"]

    aanzichtFeature = [geometryWkt, aanzichtId, aanzichtClientId, hoek, wijzigingsTimestamp, opstellingsid, informeert,
                       wegsegmentid, wegsegmentidGok,
                       datetime.now().strftime('%d/%m/%Y')]
    ##    print "aanzichtfeature = %s" %aanzichtFeature

    jsonLineBordLijst = jsonLineAanzicht["borden"]

    return aanzichtFeature, aanzichtId, jsonLineBordLijst


def leesBord(aanzichtid, opstellingsid, geometryWkt, jsonLineBord):
    bordId = jsonLineBord["id"]
    bordClientId = jsonLineBord["clientId"]
    code = (jsonLineBord["code"])
    ##    code = 'r'

    breedte = jsonLineBord["breedte"]
    hoogte = jsonLineBord["hoogte"]
    vorm = jsonLineBord["vorm"]
    folietype = jsonLineBord["folieType"]
    leverancierNaam = jsonLineBord["leverancierItem"]["naam"]
    fabricageJaar = jsonLineBord["fabricageJaar"]
    fabricageTypeNaam = jsonLineBord["fabricageType"]["naam"]
    besteknummer = jsonLineBord["besteknummer"]
    parameters = "|".join((jsonLineBord["parameters"])).replace("'", "")
    if len(parameters) > 250:
        parameters = parameters[:249] + "*"
    actief = jsonLineBord["actief"]
    try:
        beheerder = jsonLineBord["beheerder"]["gebiednummernaam"]
    except:
        beheerder = None
    datumPlaatsing = jsonLineBord["datumPlaatsing"]
    if (datumPlaatsing) == None or len(datumPlaatsing) != 10:
        datumPlaatsing = "01/01/1950"

    ##    if  (datumPlaatsing) == None:
    ##        datumPlaatsing = "01/01/1950"
    ##    elif len(datumPlaatsing) != 10:
    ##        datumPlaatsing = "01/01/1950"

    bordFeature = [geometryWkt, bordId, bordClientId, code, breedte, hoogte, vorm, folietype, leverancierNaam,
                   fabricageJaar, fabricageTypeNaam, besteknummer, parameters, actief, beheerder, datumPlaatsing,
                   aanzichtid, opstellingsid, datetime.now().strftime('%d/%m/%Y')]

    return bordFeature


def maakCursors(featureclassList, workspace):
    from arcpy import da
    edit = da.Editor(workspace)
    edit.startEditing(with_undo=False, multiuser_mode=False)
    edit.startOperation()
    print(featureclassList[0][0], featureclassList[0][1])
    icOpstelling = da.InsertCursor(featureclassList[0][0], featureclassList[0][1])
    icAanzicht = da.InsertCursor(featureclassList[1][0], featureclassList[1][1])
    icBord = da.InsertCursor(featureclassList[2][0], featureclassList[2][1])

    print("dir(edit)",dir(edit))
    return icOpstelling, icAanzicht, icBord, edit


def schrijfFeature(cursor, feature):
    try:
        cursor.insertRow(feature)
    except:
        print
        "***FOUT", feature
        cursor.insertRow(feature)

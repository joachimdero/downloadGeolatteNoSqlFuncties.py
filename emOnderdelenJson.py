# Convert GeoJson

import json, os, arcpy
from datetime import datetime

#importeer Awv functie en downloadFunctie
from sys import path
basispath =  os.path.realpath(__file__).split("awvDataTools")[0]
print "basispath = ",basispath
path2 =  os.path.join(basispath,"awvDataTools", "AwvFuncties")
path.append(path2)
import AwvFuncties
import downloadGeolatteNoSqlFuncties
reload(AwvFuncties)
try:
    importlib.reload(AwvFuncties)
    importlib.reload(downloadGeolatteNoSqlFuncties)
    arcpy.AddMessage("reload python 3")
except:
    reload (AwvFuncties)
    reload (downloadGeolatteNoSqlFuncties)
    arcpy.AddMessage("reload python 2")

#lees waarden
def leesOpstelling(jsonLine):
    #"[u'OBJECTID', u'SHAPE', u'ident8', u'gebied', u'gekozenbeheerder', u'begindatum', u'actueleOpstelling', u'berekendeBeheerder', u'id']"
    x = jsonLine["geometry"]["coordinates"][0]
    y = jsonLine["geometry"]["coordinates"][1]
    geometryWkt = "POINT (%s %s)" %(x,y)

    jsonLineProperties = jsonLine["properties"]


    veldenLijst = [
    'ident8',
    'positie',
    'zijde',
    'typeNaam',
    'typeOmschrijving',
    'status',
    'installatieNaampad',
    'laatsteUpdate',
    'rootPad',
    'site',
    'updateUserRef',
    'beheerderBhRef',
    'commentaar',
    'bronid',
    'toezichter',
    'CopyDatum'
    ]

    veldenDict = {}

    siteDict = {}

    if "site" in jsonLineProperties and (jsonLineProperties["site"]) != None and "ident8:" in jsonLineProperties["site"].lower():
##        print jsonLineProperties["site"]
        for f in jsonLineProperties["site"].split(";"):
            siteDict[f.split(":")[0].lower().strip(" ")] = f.split(":")[1].strip(" ")
##    elif "site" in jsonLineProperties and (jsonLineProperties["site"]) != None and "- ident8" in jsonLineProperties["site"].lower():
##        print jsonLineProperties["site"]
##        for f in jsonLineProperties["site"].split("-"):
##            print f
##            siteDict[f.split(":")[0].lower().strip(" ")] = f.split(":")[1].strip(" ")
##        print "siteDict = ", siteDict

    for veld in veldenLijst:
        try:
            if veld == 'ident8':
                veldenDict[veld] = siteDict[veld]

            elif veld == 'positie' and (siteDict['kilometerpunt']) != "":
                veldenDict[veld] = siteDict['kilometerpunt']
            elif veld == 'zijde':
                veldenDict[veld] = siteDict['zijde weg']
            elif veld == 'site':
                veldenDict[veld] = jsonLineProperties[veld][:250]
            else:
                veldenDict[veld] = jsonLineProperties[veld][:250]

        except:
            veldenDict[veld] = None

    opstellingsFeature = [
    geometryWkt,
    veldenDict["ident8"],
    veldenDict["positie"],
    veldenDict["zijde"],
    veldenDict["typeNaam"],
    veldenDict["typeOmschrijving"],
    veldenDict["status"],
    veldenDict['installatieNaampad'],
    Datum(veldenDict["laatsteUpdate"]),
    veldenDict["rootPad"],
    veldenDict["site"],
    veldenDict["updateUserRef"],
    veldenDict["beheerderBhRef"] ,
    veldenDict["commentaar"] ,
    veldenDict["bronid"],
    veldenDict["toezichter"],
    datetime.now().strftime('%d/%m/%Y')
    ]

##    print ("opstellingsfeature = %s" %opstellingsFeature)
    return opstellingsFeature


def maakCursors(featureclassList, workspace):
    from arcpy import da
    edit = da.Editor(workspace)
    edit.startEditing(True, True)
    edit.startOperation()
    icOpstelling = da.InsertCursor(featureclassList[0][0], featureclassList[0][1])


    return icOpstelling, edit

def Datum(datumstring):
    # datumstring in, datum uit
    from datetime import datetime

    if "T" in datumstring:
        datumstring = datumstring.split("T")[0]
    if ' ' in datumstring:
        datumstring = datumstring.split(' ')[0]


    try:
        datum = datetime.strptime(datumstring, '%Y-%m-%d')
    except:
        try:
            datum = datetime.strptime(datumstring, '%d/%m/%Y')
        except:
            datum = datetime.strptime('1950-01-01', '%Y-%m-%d')

    return datum


def schrijfFeature(cursor, feature):
##    print cursor
##    print feature
    try:
        cursor.insertRow(feature)
    except:
        print "***FOUT",feature
        cursor.insertRow(feature)

# Convert GeoJson

# Parameters




#lees waarden
def leesOpstelling(jsonLine):
    #"[u'OBJECTID', u'SHAPE', u'ident8', u'gebied', u'gekozenbeheerder', u'begindatum', u'actueleOpstelling', u'berekendeBeheerder', u'id']"
    x = jsonLine["geometry"]["coordinates"][0]
    y = jsonLine["geometry"]["coordinates"][1]
    geometryWkt = "POINT (%s %s)" %(x,y)

    jsonLineProperties = jsonLine["properties"]


    veldenLijst = [
    'ident8',
    'positie',
    'zijde',
    'typeNaam',
    'typeOmschrijving',
    'status',
    'installatieNaampad',
    'laatsteUpdate',
    'rootPad',
    'site',
    'updateUserRef',
    'beheerderBhRef',
    'commentaar',
    'bronid',
    'toezichter',
    'CopyDatum'
    ]

    veldenDict = {}

    siteDict = {}

    if "site" in jsonLineProperties and (jsonLineProperties["site"]) != None and "ident8:" in jsonLineProperties["site"].lower():
##        print jsonLineProperties["site"]
        for f in jsonLineProperties["site"].split(";"):
            siteDict[f.split(":")[0].lower().strip(" ")] = f.split(":")[1].strip(" ")
##    elif "site" in jsonLineProperties and (jsonLineProperties["site"]) != None and "- ident8" in jsonLineProperties["site"].lower():
##        print jsonLineProperties["site"]
##        for f in jsonLineProperties["site"].split("-"):
##            print f
##            siteDict[f.split(":")[0].lower().strip(" ")] = f.split(":")[1].strip(" ")
##        print "siteDict = ", siteDict

    for veld in veldenLijst:
        try:
            if veld == 'ident8':
                veldenDict[veld] = siteDict[veld]

            elif veld == 'positie' and (siteDict['kilometerpunt']) != "":
                veldenDict[veld] = siteDict['kilometerpunt']
            elif veld == 'zijde':
                veldenDict[veld] = siteDict['zijde weg']
            elif veld == 'site':
                veldenDict[veld] = jsonLineProperties[veld][:250]
            else:
                veldenDict[veld] = jsonLineProperties[veld][:250]

        except:
            veldenDict[veld] = None

    opstellingsFeature = [
    geometryWkt,
    veldenDict["ident8"],
    veldenDict["positie"],
    veldenDict["zijde"],
    veldenDict["typeNaam"],
    veldenDict["typeOmschrijving"],
    veldenDict["status"],
    veldenDict['installatieNaampad'],
    Datum(veldenDict["laatsteUpdate"]),
    veldenDict["rootPad"],
    veldenDict["site"],
    veldenDict["updateUserRef"],
    veldenDict["beheerderBhRef"] ,
    veldenDict["commentaar"] ,
    veldenDict["bronid"],
    veldenDict["toezichter"],
    datetime.now().strftime('%d/%m/%Y')
    ]

##    print ("opstellingsfeature = %s" %opstellingsFeature)
    return opstellingsFeature


def maakCursors(featureclassList, workspace):
    from arcpy import da
    edit = da.Editor(workspace)
    edit.startEditing(True, True)
    edit.startOperation()
    icOpstelling = da.InsertCursor(featureclassList[0][0], featureclassList[0][1])


    return icOpstelling, edit



def schrijfFeature(cursor, feature):
##    print cursor
##    print feature
    try:
        cursor.insertRow(feature)
    except:
        print "***FOUT",feature
        cursor.insertRow(feature)


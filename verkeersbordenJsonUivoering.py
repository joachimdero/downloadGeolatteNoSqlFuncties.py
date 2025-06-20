import sys

from sys import path

path.append(r"D:\GoogleDrive\SyncNoSqlFs\GIStools\download_wdb")
import verkeersbordenJson

from arcpy import TruncateTable_management

import json, os

ospj = os.path.join

in_json = r"C:\awvData\downloadGeolatteNoSqlFeatureserver\verkeersborden.json"

gdb1 = r"C:\awvData\AwvData.gdb"
gdb = "in_memory"
fc_opstelling = "vkbOpstelling"
fc_aanzicht = "vkbAanzicht"
fc_bord = "vkbBord"

# maak fc in memory
for fc in (fc_opstelling, fc_aanzicht, fc_bord):
    arcpy.CreateFeatureclass_management(gdb, fc, "POINT", ospj(gdb1, fc))

# velden aanwezig in de doelbestanden (featureclasses). Volgorde wordt gebruikt in de cursors
f_opstelling = [
    'SHAPE@WKT',
    'opstellingsid',
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
    'copyDatum'
]

f_aanzicht = [
    'SHAPE@WKT',
    'aanzichtId',
    'aanzichtClientId',
    'hoek',
    'wijzigingsTimestamp',
    "opstellingsid",
    "informeert",
    "wegsegmentid",
    "wegsegmentidGok",
    'copyDatum'
]

f_bord = [
    'SHAPE@WKT',
    'bordId',
    'bordClientId',
    'code', 'breedte',
    'hoogte',
    'vorm',
    'folietype',
    'leverancierNaam',
    'fabricageJaar',
    'fabricageTypeNaam',
    'besteknummer',
    'parameters',
    'actief',
    'beheerder',
    'datumPlaatsing',
    'aanzichtid'
    , 'opstellingsid',
    'copyDatum'
]

# maak cursors
featureclassList = [
    [ospj(gdb, fc_opstelling), f_opstelling],
    [ospj(gdb, fc_aanzicht), f_aanzicht],
    [ospj(gdb, fc_bord), f_bord]
    ]
ic_opstelling, ic_aanzicht, ic_bord,edit = verkeersbordenJson.maakCursors(
    featureclassList=featureclassList,
    workspace=gdb
)

# lees jsonfile
try:
    with open(in_json) as OpenJson:
        for i,line in enumerate(OpenJson):
            if i > 5:
                pass
            if i in range(0, 1000000000, 10000):
                sys.stdout.write("\r%s lijnen in jsonfile gelezen"%i)
                sys.stdout.flush()

            jsonLine = json.loads(line)

            opstellingsFeature, opstellingsId, geometryWkt, jsonLineAanzichtLijst = verkeersbordenJson.leesOpstelling(
                jsonLine)

            verkeersbordenJson.schrijfFeature(ic_opstelling, opstellingsFeature)

            for jsonLineAanzicht in jsonLineAanzichtLijst:
                aanzichtFeature, aanzichtId, jsonLineBordLijst = verkeersbordenJson.leesAanzicht(opstellingsId,
                                                                                                 geometryWkt,
                                                                                                 jsonLineAanzicht)
                verkeersbordenJson.schrijfFeature(ic_aanzicht, aanzichtFeature)

                for jsonLineBord in jsonLineBordLijst:
                    bordFeature = verkeersbordenJson.leesBord(aanzichtId, opstellingsId, geometryWkt, jsonLineBord)
                    verkeersbordenJson.schrijfFeature(ic_bord, bordFeature)

except Exception as e:
    print("line:", line)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    raise

print("stop editing")
del ic_opstelling, ic_aanzicht, ic_bord
#edit.stopOperation()
edit.stopEditing(save_changes=True)


# maak featureclasse leeg
for relation in ("vkbOpstellingAanzicht", "vkbAanzichtBord", "vkbOpstellingBord"):
    arcpy.Delete_management(ospj(gdb1, relation))

for fc in (fc_opstelling, fc_aanzicht, fc_bord):
    print("maak fc %s leeg" % fc)
    fcPath = ospj(gdb1, fc)
    TruncateTable_management(fcPath)
for fc in (fc_opstelling, fc_aanzicht, fc_bord):
    print("zet data %s over" % fc)
    fcPath = ospj(gdb1, fc)
    arcpy.Append_management(ospj(gdb, fc), ospj(gdb1, fc))

for relation in (
        [ospj(gdb1, fc_opstelling), ospj(gdb1, fc_aanzicht), ospj(gdb1, "vkbOpstellingAanzicht"), "vkbAanzicht",
         "vkbOpstelling", "opstellingsid", ],
        [ospj(gdb1, fc_aanzicht), ospj(gdb1, fc_bord), ospj(gdb1, "vkbAanzichtBord"), "vkbBord", "vkbAanzicht",
         "aanzichtId"],
        [ospj(gdb1, fc_opstelling), ospj(gdb1, fc_bord), ospj(gdb1, "vkbOpstellingBord"), "vkbOpstelling", "vkbBord",
         "opstellingsid"]
):
    print("maak relatie %s aan" % relation[2])
    arcpy.CreateRelationshipClass_management(
        relation[0], relation[1], relation[2], "SIMPLE", relation[3], relation[4],
                                             "BOTH", "ONE_TO_MANY", "NONE", relation[5], relation[5]
                                             )



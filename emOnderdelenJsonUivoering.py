# Convert GeoJson

# Parameters

from sys import path
path.append(r"D:\GoogleDrive\SyncNoSqlFs\GIStools\download_wdb")
import emOnderdelenJson

from arcpy import TruncateTable_management, GetCount_management

import json, os
ospj = os.path.join

##inputJson = r"D:\GoogleDrive\SyncNoSqlFs\GISdata\CsvGeolatteNoSqlFeatureserver\emonderdelen.json"
##inputJson = r'Q:\Mijn Drive\Team GIS\GISdata\CsvGeolatteNoSqlFeatureserver\emonderdelen.json'
basispath =  os.path.realpath(__file__).split("awvDataTools")[0]
print "realpath = ",os.path.realpath(__file__)
print "basispath = ",basispath
path =  ospj(basispath,"downloadGeolatteNoSqlFeatureserver")
inputJson =  ospj(path, "emonderdelen.json")

gdb = os.path.join(basispath,"AwvData.gdb")
##gdb = r"D:\GoogleDrive\SyncNoSqlFs\GISdata\AwvData.gdb"
##gdb = r"C:\GoogleDrive\01_direct_te_verwijderen\new_file_geodatabase.gdb"
gdbTemp = "in_memory"
fc = "emOnderdelen"

#maak fc in memory
for fc in(fc,):
    print ("Maak fc in memory voor %s"% fc)
    arcpy.CreateFeatureclass_management(gdbTemp,fc,"POINT",ospj(gdb,fc))

#velden aanwezig in de doelbestanden (featureclasses). Volgorde wordt gebruikt in de cursors
fieldsOpstelling = [
    'SHAPE@WKT',
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

#maak cursors
print ("Maak cursor")
featureclassList = [ [ospj(gdbTemp,fc),fieldsOpstelling] ]
icOpstelling, edit = emOnderdelenJson.maakCursors(featureclassList, gdbTemp)


#lees jsonfile
teller = 0
##try:
with open(inputJson) as OpenJson:
    for line in OpenJson:
        teller += 1
        if int(teller/50000.0) == teller/50000.0:
            print "%s lijnen in jsonfile gelezen" %teller
##                break
        if len(str(line)) < 5:
            print "break"
            break
##        print "\n"
##        print ("jsonLine: %s" %line)
        jsonLine =  json.loads(line)



        opstellingsFeature = emOnderdelenJson.leesOpstelling(jsonLine)
##        print "opstellingsFeature : %s" % opstellingsFeature
        emOnderdelenJson.schrijfFeature(icOpstelling,opstellingsFeature)

##except Exception as e:
##    print "line",line
####    print "linenext", line, OpenJson.next()
##    exc_type, exc_obj, exc_tb = sys.exc_info()
##    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
##    print(exc_type, fname, exc_tb.tb_lineno)
##    raise


print "stop editing"
edit.stopOperation()
edit.stopEditing(True)

del icOpstelling
print 'del'

# maak featureclasse leeg
for fc in(fc,):
    fcPath = ospj(gdb,fc)
    print (GetCount_management(fcPath))
    print "maak fc %s leeg, %s" % (fc, fcPath)
    TruncateTable_management(fcPath)
    print (GetCount_management(fcPath))
##for fc in(fc,):
    print "zet data %s over" %fc
    fcPath = ospj(gdb,fc)
    arcpy.Append_management(ospj(gdbTemp,fc), ospj(gdb,fc))
    print (GetCount_management(fcPath))


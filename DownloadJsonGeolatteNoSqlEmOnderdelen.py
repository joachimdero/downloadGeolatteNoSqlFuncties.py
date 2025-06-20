import os, importlib
from arcpy import AddMessage
arcpy.AddMessage = AddMessage
from sys import path

#importeer Awv functie en downloadFunctie
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


#parameters
omgeving ='productie' #'dev','tei'
urlOmgeving = 'apps'
if omgeving in ('dev','tei'):
    urlOmgeving = urlOmgeving + '-' + omgeving
urlNoSqlFeatureserverEm = 'https://services.%s.mow.vlaanderen.be/geolatte-nosqlfs/cert/api/databases/eminfra/' %urlOmgeving

outputMap =  r"C:\awvData\downloadGeolatteNoSqlFeatureserver"
certMap = r"C:\awvData\awvDataTools"

# lagen
lagenDict = {
 'emonderdelen':[],
}


AwvFuncties.PrintLogMessage("\nStart downloadJsonEmOnderdelen" , r"C:\awvData\awvDataTools\log\logDownloadCsvGeolatteNoSqlEvents.txt")
opener = AwvFuncties.Certificaat(omgeving)

for laag in lagenDict:
    outputJson = os.path.join(outputMap,laag+".json")
    BasisUrl = urlNoSqlFeatureserverEm
    print "BasisUrl = %s" % BasisUrl
    projection = ""
    downloadGeolatteNoSqlFuncties.DownloadJson(laag,outputJson,BasisUrl, projection, opener)
    AwvFuncties.PrintLogMessage("\nStop downloadJsonEmOnderdelen" , r"C:\awvData\awvDataTools\log\logDownloadCsvGeolatteNoSqlEvents.txt")

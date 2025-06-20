# oplijsten beschikbare lagen in featureserver
import urllib,json, csv

# download data
UrlFeatureserver = 'http://apps.awv.vonet.be/featureserver/rest/tables'
omgeving = "productie"
OutputMap = "//WSE148445M/MOW_AWV_1/06_gis/share/GISdata/CsvFeatureserver/"
##OutputMap = "d:/"

print UrlFeatureserver
Downloadfunctie = urllib.URLopener()
Leesfunctie = urllib.urlopen('http://apps.awv.vonet.be/featureserver/rest/tables')
Downloadfunctie.retrieve(UrlFeatureserver, OutputMap+'InhoudFeatureserver'+".json")
print 'download geslaagd'
text = json.loads(Leesfunctie.read())

with open(OutputMap + 'DataFeatureserver.txt', "w") as text_file:
    text_file.write("Featureserver bevat geexporteerde data uit wdb met uitzondering van verkeersborden,\n\
aangevuld met niet rechtstreeks ontsloten data die getoond worden in wdb zoals staat van de weg.\n\n\
Dit zijn de beschikbare datalagen: \n")
    for item in (text["items"]):
        print item["name"]
        text_file.write(item["name"]+ "\n")


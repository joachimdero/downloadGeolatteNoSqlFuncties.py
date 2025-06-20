# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      derojp
#
# Created:     17/08/2020
# Copyright:   (c) derojp 2020
# Licence:     <your licence>
# -------------------------------------------------------------------------------
import logging
import os, datetime
import sys
from datetime import datetime

import requests


def DownloadCsvGepagineerd(report, outputCsv, basisUrl, opener, projection, start, limit):
    # gepagineerd downloaden duurt langer wanneer ja alles wil hebben doordar de server eerst moet sorteren, niet meer standaard in gebruik vanaf 20201125
    # download data uit featureserver
    import urllib2
    print('***start download %s en sla op als bestand %s***' % (report, outputCsv))
    status = 'uitvoeren'
    outputCsv = open(outputCsv, 'wb')
    Header = 'ja'

    while status == 'uitvoeren':
        print('download van ', start, 'tot', start + limit)
        url = '%s/query?fmt=csv&sep=%%7C&start=%s&limit=%s&withgeometry=true&projection=%s' % (
            basisUrl + report, start, limit, projection)
        if start == 0:
            print("Url = %s" % url)

        if report == 'GenummerdeWegenWdb':
            url = basisUrl
            req = urllib2.Request(url)
        else:
            req = urllib2.Request(url, headers={"Accept": "application/vnd.geolatte-featureserver+csv"})

        teller = 1
        while teller < 10:
            try:
                response = opener.open(req).read()
                teller = 10
            except (Exception, e):
                print(e)
                teller += 1
                print('-------------%sde poging---------------------' % teller)

        # schrijf veldnamen niet mee weg
        if Header == 'nee':
            response = (('\n').join(response.splitlines()[1:])) + '\n'
        outputCsv.write(response)
        start += limit
        Header = 'nee'
        httpStatus = opener.open(req).getcode()
        if httpStatus != 200 or len(
                str(response)) < 100 or start > 3000000 or report == 'GenummerdeWegenWdb':  # stel hier een stop in om te testen
            status = 'stop'
            print('stop')

    outputCsv.close()


def DownloadCsvStream(report, outputCsv, basisUrl, opener, projection):
    # download data uit featureserver
    import urllib2
    print('***start download %s en sla op als bestand %s***' % (report, outputCsv))
    status = 'uitvoeren'
    outputCsv = open(outputCsv, 'wb')
    Header = 'ja'

    url = '%s/query?fmt=csv&sep=%%7C&withgeometry=true&projection=%s' % (basisUrl + report, projection)
    print("Url = %s" % url)

    if report == 'GenummerdeWegenWdb':
        url = basisUrl
        req = urllib2.Request(url)
    else:
        req = urllib2.Request(url, headers={"Accept": "application/vnd.geolatte-featureserver+csv"})

    teller = 1
    while teller < 10:
        try:
            rsp = opener.open(req)
            data = rsp.read(8192)  # stream data
            outputCsv.write(data)
            while data:  # stream data
                # .. Do Something ..
                data = rsp.read(8192)  # stream data
                outputCsv.write(data)

            teller = 10
        except Exception as e:
            print(e)
            teller += 1
            print('-------------%sde poging---------------------' % teller)

    outputCsv.close()


def DownloadCsv(report, outputCsv, basisUrl, opener, projection):
    # download data uit featureserver
    import urllib2
    print('***start download %s en sla op als bestand %s***' % (report, outputCsv))
    status = 'uitvoeren'
    outputCsv = open(outputCsv, 'wb')
    Header = 'ja'

    while status == 'uitvoeren':
        url = '%s/query?fmt=csv&sep=%%7C&withgeometry=true&projection=%s' % (basisUrl + report, projection)
        print("Url = %s" % url)

        if report == 'GenummerdeWegenWdb':
            print("report = genummerde wegen")
            url = basisUrl
            print("Url = %s" % url)
            req = urllib2.Request(url)
        else:
            req = urllib2.Request(url, headers={"Accept": "application/vnd.geolatte-featureserver+csv"})

        teller = 1

        ##        response = opener.open(req).read()
        while teller < 10:
            try:
                response = opener.open(req).read()
                teller = 10
            except Exception as e:
                print(e)
                teller += 1
                print('-------------%sde poging---------------------' % teller)

        # schrijf veldnamen niet mee weg
        if Header == 'nee':
            response = (('\n').join(response.splitlines()[1:])) + '\n'
        outputCsv.write(response)
        Header = 'nee'
        httpStatus = opener.open(req).getcode()
        if httpStatus != 200 or len(
                str(response)) < 100 or report == 'GenummerdeWegenWdb':  # stel hier een stop in om te testen
            status = 'stop'
            print('stop')
        else:
            print("download is klaar")
            status = 'stop'

    outputCsv.close()


def DownloadJsonGepagineerd(laag, outputJson, BasisUrl, projection, opener, start, limit):
    # download data uit featureserver
    import urllib2
    print('***start download %s***' % laag)
    start = 0
    status = 'uitvoeren'
    outputJsonOpen = open(outputJson, 'wb')
    Header = 'ja'
    while status == 'uitvoeren':
        Url = '%s/query?fmt=json&start=%s&limit=%s&projection=%s' % (BasisUrl + laag, start, limit, projection)
        req = urllib2.Request(Url, headers={"Accept": "application/vnd.geolatte-featureserver+json"})
        if start == 0:
            print(Url)

        teller = 1
        while teller < 10:
            try:
                response = opener.open(Url).read()
                teller = 10
            except:
                teller += 1
                response = opener.open(Url).read()
                print('-------------%sde poging---------------------' % teller)

        outputJsonOpen.write(response)
        start += limit
        print('download van ', start, 'tot', start + limit)
        Header = 'nee'
        if "/" not in response or start > 2000000 or laag == 'GenummerdeWegenWdb':
            status = 'stop'
            print('stop')

    outputJsonOpen.close()


def download_json_stream(laag, out_json, url_basis, projection, session):  # STREAM
    import requests

    session.headers.update({'Content-Type': 'application/json', 'accept': 'application/json'})
    print('***start download %s***' % laag)
    url = '%s/query?fmt=json&projection=%s' % (url_basis + laag, projection)
    print(f"url: {url}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Controleer of er geen fouten zijn
        total_size = int(r.headers.get('Content-Length', 0))  # Totale grootte van het bestand in bytes
        print(f"total_size: {total_size}")
        downloaded_size = 0
        printed_size = 0
        chunk_size = 1024 * 1024  # Grootte van elke chunk (1MB)
        with open(out_json, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):  # Download het bestand in blokken van 8KB
                if chunk:  # Filter out keep-alive new chunks
                    f.write(chunk)  # Schrijf elk blok naar het lokale bestand
                    downloaded_size += len(chunk)
                    if round((int(downloaded_size)) / (1024 * 1024), 0) - printed_size > 4.9:
                        printed_size = round((int(downloaded_size)) / (1024 * 1024), 0)
                        sys.stdout.write(f"\r{printed_size} MB gedownload")
                        sys.stdout.flush()
                        # print(f"{printed_size} MB gedownload")
    print(f"Download voltooid: {out_json}")
    return downloaded_size


def download_json_stream_new(laag, out_json, url_basis, projection, session):
    """
    Downloadt een JSON-bestand via streaming en slaat het lokaal op.

    Args:
        laag (str): Naam van de laag in de API.
        out_json (str): Pad naar het uitvoerbestand.
        url_basis (str): Basis-URL van de API.
        projection (str): Gegevensprojectie.
        session (requests.Session): Actieve requests-session met eventuele authenticatie.

    Returns:
        int: Aantal gedownloade bytes.
    """

    url = f"{url_basis}{laag}/query?fmt=json&projection={projection}"

    try:
        print(f"session.headers:{session.headers}")
        r = session.oauth_get(url=url,  stream=True)
        r.raise_for_status()  # Stop bij HTTP-fout

        downloaded_size = 0
        printed_size = 0
        chunk_size = 1024 * 1024  # 1 MB

        with open(out_json, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    # Update progress bij elke 5 MB
                    if round(downloaded_size / (1024 * 1024), 0) - printed_size > 4.9:
                        printed_size = round(downloaded_size / (1024 * 1024), 0)
                        sys.stdout.write(f"\r {printed_size} MB gedownload...")
                        sys.stdout.flush()

        print(f"\nDownload voltooid: {out_json}")
        logging.info(f"download laag {laag} geslaagd: {downloaded_size / (1024 * 1024)} Mb, path: {out_json}")
        return downloaded_size

    except requests.RequestException as e:
        print(f"\n❌ Fout bij downloaden van {laag}: {e}")
        return 0
    except Exception as e:
        print(e)


def DownloadJson(laag, outputJson, url_basis, projection, opener):  # STREAM
    import urllib2
    print('***start download %s***' % laag)
    url = '%s/query?fmt=json&projection=%s' % (url_basis + laag, projection)
    print(url)

    outputJsonOpen = open(outputJson, 'wb')
    req = urllib2.Request(url, headers={"Accept": "application/vnd.geolatte-featureserver+json"})
    rsp = opener.open(req)
    total_size = rsp.info().get('Content-Length')
    print('total_size: %s' % total_size)
    ##    rsp = urllib2.urlopen(req)
    global data
    chunk_size = 8192  # origineel
    chunk_size = 131072
    print_interval = 9
    data = rsp.read(chunk_size)  # stream data
    outputJsonOpen.write(data)

    downloaded_size = 0
    while data:  # stream data
        try:
            downloaded_size += len(data)
            if downloaded_size % (chunk_size * print_interval) == 0:
                print(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
                print("downloaded_size: %s MB" % ((int(downloaded_size)) / (1024 * 1024)))

            data = rsp.read(chunk_size)  # stream data
            outputJsonOpen.write(data)
        except (Exception, e):
            print("An unexpected error occurred: ", str(e))

    outputJsonOpen.close()


def refreshData(fc, gdb, shpPath, field_mapping):
    RefreshDataGdb(fc, gdb)
    if arcpy.Exists(os.path.join(shpPath, fc)):
        refreshShp(fc, shpPath, field_mapping)
    else:
        arcpy.FeatureClassToFeatureClass_conversion(fc, shpPath, fc, field_mapping=field_mapping)


def refreshShp(fc, shpPath, field_mapping):
    arcpy.DeleteFeatures_management(os.path.join(shpPath, fc))
    arcpy.Append_management(fc, os.path.join(shpPath, fc), schema_type="NO_TEST", field_mapping=field_mapping)


def truncateTable(WsOutput, csvFile):
    try:
        arcpy.TruncateTable_management(os.path.join(WsOutput, csvFile))
        countFeatures = int(str(arcpy.GetCount_management(os.path.join(WsOutput, csvFile))))
        print("truncate table uitgevoerd %s elementen aanwezig" % countFeatures)
    except (Exception, e):
        print("truncate table niet gelukt voor %s" % os.path.join(WsOutput, csvFile))
        arcpy.TruncateTable_management(os.path.join(WsOutput, csvFile))

        countFeatures = int(str(arcpy.GetCount_management(os.path.join(WsOutput, csvFile))))
        if countFeatures >= 1:
            print(
                "nog records aanwezig in table. de table wordt met Delete Features leeggemaakt en truncate table wordt opnieuw uitgevoerd, nog %s records aanwezig" % countFeatures)
            arcpy.DeleteRows_management(os.path.join(WsOutput, csvFile))
            arcpy.TruncateTable_management(os.path.join(WsOutput, csvFile))


def RefreshDataGdb(fc_table, out_ws):
    ##	arcpy.CopyFeatures_management(csvFile, WsTest + csvFile)
    print("Refresh Data voor laag %s in omgeving %s" % (fc_table, out_ws))

    def TruncateTable(out_ws, out_fc_table):
        import arcpy
        try:
            arcpy.TruncateTable_management(os.path.join(out_ws, out_fc_table))
            countFeatures = int(str(arcpy.GetCount_management(os.path.join(out_ws, out_fc_table))))
            print("truncate table uitgevoerd %s elementen aanwezig" % countFeatures)
        except (Exception, e):
            print("truncate table niet gelukt voor %s" % os.path.join(out_ws, out_fc_table))
            arcpy.TruncateTable_management(os.path.join(out_ws, out_fc_table))

        countFeatures = int(str(arcpy.GetCount_management(os.path.join(out_ws, out_fc_table))))
        if countFeatures >= 1:
            print(
                "nog records aanwezig in table. de table wordt met Delete Features leeggemaakt en truncate table wordt opnieuw uitgevoerd, nog %s records aanwezig" % countFeatures)
            arcpy.DeleteRows_management(os.path.join(out_ws, out_fc_table))
            arcpy.TruncateTable_management(os.path.join(out_ws, out_fc_table))

    TruncateTable(out_ws, fc_table)
    print('start data toevoegen bron (memory): %s, doel: %s' % (fc_table, os.path.join(out_ws, fc_table)))
    print('*')
    arcpy.Append_management(fc_table, os.path.join(out_ws, fc_table))
    print('*')
    print("aantal features in memory tabel:", arcpy.GetCount_management(fc_table))
    print("aantal features in definitieve tabel:", arcpy.GetCount_management(os.path.join(out_ws, fc_table)))
    print('***refresh %s %s geslaagd***' % (fc_table, out_ws))


def RefreshDataError(csv_file, ws_output):
    fields_csvfile = [[field.name] + [field.type] for field in arcpy.ListFields(csv_file)]
    fields_fc = [[field.name] + [field.type] for field in arcpy.ListFields(os.path.join(ws_output, csv_file))]

    for field in fields_csvfile:
        if field not in fields_fc:
            print("veld %s ontbreekt in %s" % (field, os.path.join(ws_output, csv_file)))
    for field in fields_fc:
        if field not in fields_csvfile:
            print("veld %s wordt niet ingevuld" % field)
    print("einde RefreshDataError")


def MaakTabel(row, WsMem, csvFile, out_map_table, dataType):
    import arcpy
    print("veldnamen inlezen van csv %s" % csvFile)

    # veldnamen bepalen en rijen inlezen
    ListFieldName, DictFieldNameFClass, ListFieldNameFClass = [], {}, []
    # maak tabel aan
    M, Z = "ENABLED", "ENABLED"
    if dataType == "table":
        arcpy.CreateTable_management(WsMem, csvFile)
    elif dataType == "point":
        arcpy.CreateFeatureclass_management(WsMem, csvFile, "POINT", has_z=Z, has_m=Z)
    elif dataType == "line":
        M, Z = "DISABLED", "DISABLED"
        if csvFile in ('GenummerdeWegenWdb'):
            M, Z = "ENABLED", "ENABLED"
        arcpy.CreateFeatureclass_management(WsMem, csvFile, "POLYLINE", has_z=Z, has_m=Z)

    # vul eerste rij aan met bijkomende veldnamen
    ##    print ("row veldnamen = %s" %row)

    if "wijzigingsdatum" in row or "laatsteUpdate" in row:
        row.extend(("CopyDatum", "DeltaWijzigingsDatum"))
    else:
        row.append(("CopyDatum"))

    # vervang veldnamen
    for FieldName in row:
        ##        print ("FieldName = %s" %FieldName)
        # wijzig veldnaam indien nodig: verwijder wat voor het punt staat, verwijder ook punten indien nodig
        FieldName = FieldName.replace('locatie.', '').replace('geometry-wkt', 'geometry_wkt').replace(
            'materiaalpaal.naam', 'materiaalpaal').replace('nederlandseNaam', 'NederlandseNaam').replace('.',
                                                                                                         '').replace(
            "doorgangTypeUitzonderlijkVervoer", "doorgangTypeUitzonderlijkVervoe")
        ListFieldName.append(FieldName)
        ##        print ("FieldName = %s" %FieldName)
        # doe niets voor volgende veldnamen
        if FieldName in (
                "shape", "geometry", 'geometry_wkt', 'geometry-wkt', 'offsetdirection', 'offsetdirection_wkt', 'offset',
                'offsetZijde', 'type', 'zijderijweg', '_id'):  # 'wegcategorie',
            ##            print ("continue")
            continue
        ##        print ("doe voort")

        # veldlengte
        DictFieldLength = {
            "ident8": 8,
            "wegnummer": 8,
            'gebruiker': 40,
            "gebied": 100,
            "omschrijving": 3500}
        FieldLength = 250  # standaard
        if FieldName in DictFieldLength:
            FieldLength = DictFieldLength.get(FieldName, "none")

        # veldtype
        FieldType = 'Text'  # standaard
        if FieldName in (
                'DeltaWijzigingsDatum', 'breedterijbaan', 'lengte', 'hm', 'referentiepaal', 'referentie', 'beginhm',
                'beginverplaatsing', 'beginverpl', 'beginpositie', 'beginposit', 'eindhm', 'eindverplaatsing',
                'eindverpl',
                'eindpositie', 'eindpositi', 'verplaatsing', 'verpl', 'afstand', 'positie', 'van_referentiepaal',
                'tot_referentiepaal', 'van_afstand', 'tot_afstand', 'beginafstand', 'beginopschrift', 'beginpositie',
                'eindafstand', 'eindopschrift', 'eindpositie', 'bijkomendelocatiebeginhm Text 250',
                'bijkomendelocatiebeginverpl Text 250', 'bijkomendelocatieeindhm', 'bijkomendelocatieeindverpl',
                'bijkomendelocatiebeginhm', 'bijkomendelocatiebeginverpl', 'werkelijkelengte', 'tussenafstandpalen',
                'werkingsbreedte', 'minimaleinstallatielengte', 'opschrift'):
            FieldType = "Double"
        elif FieldName in ('id', 'bronid', 'aantalrijstroken', 'snelheidsbeperking',
                           'afstandrij', 'werfid', 'innamezoneid', '_id', 'aantaldiagonalen', 'aantalplanken',
                           'breedte', 'afstandrijbaan', 'parentId', 'besteknummer', 'dossiernummer',
                           'historianummer', 'wegsegmentId', 'snelheid', 'vorigeWegsegmentId', 'zonesnelheid',
                           'zone_bordType_naam'):
            FieldType = 'Long'
        elif ('datum' in FieldName or 'Datum' in FieldName or FieldName in (
                "laatsteUpdate", 'gewijzigd_op')) and FieldName != 'DeltaWijzigingsDatum':
            FieldType = 'Date'
        # veldtype uitzonderingen
        if csvFile in ('referentiepunten2',):
            if FieldName in ("id",):
                FieldType = 'Text'
                FieldLength = 20
            if FieldName in ("wegnummer",):
                FieldType = 'Text'
                FieldLength = 80

        # maak veld aan
        arcpy.AddField_management(out_map_table, FieldName, FieldType, "#", "#", FieldLength, "#", "#", "#", "#")
        DictFieldNameFClass[FieldName] = FieldType
        ListFieldNameFClass.append(FieldName)

    if dataType != "table":
        DictFieldNameFClass["SHAPE@WKT"] = "geometry"  # Shapeveld toevoegen aan dictionary om cursor aan te maken
        ListFieldNameFClass.append("SHAPE@WKT")  # Shapeveld toevoegen aan lijst om cursor aan te maken

    ## ("vprintelden in tabel %s : %s" %(out_map_table , [f.name for f in arcpy.ListFields(out_map_table)]))
    ##    print ListFieldNameFClass, DictFieldNameFClass, ListFieldName

    return ListFieldNameFClass, DictFieldNameFClass, ListFieldName


def Geometry4dTo2D(pointWKT):
    pointCoordinateList = pointWKT.split("(")[-1].strip(")").split(" ")
    pointWKT2d = "POINT(%s %s)" % (pointCoordinateList[0], pointCoordinateList[1])
    return pointWKT2d


def Wkt2DTo4D(multilineWkt2D):
    # voegt m en of z waarde toe aan xy coordinaten
    multiLineString = multilineWkt2D.split("(", 1)[1].rsplit(")", 1)[0]
    lineStringList = multiLineString.strip("(").strip(")").replace(", ", ",").split("),(")
    ##    print "lineStringList",lineStringList

    newLineStringList = []
    newMultiLineStringList = []
    lineStringTeller = 0

    for linesString in lineStringList:
        newLineStringList = []

        for coordinateString in linesString.strip(" ").split(","):
            coordinateList = coordinateString.strip(" ").split(" ")
            while len(coordinateList) < 4:
                coordinateList.append(0)
            newCoordinateString = " ".join((str(i) for i in coordinateList))
            newLineStringList.append(newCoordinateString)

        newLinesString = "(%s)" % ",".join(newLineStringList)
        newMultiLineStringList.append(newLinesString)

    newMultiLineString = ",".join(newMultiLineStringList)

    multilineWKT4d = multilineWkt2D.replace(multiLineString, "").replace("()", "(%s)" % newMultiLineString)
    if "ZM" not in multilineWKT4d:
        multilineWKT4d = multilineWKT4d.replace("MULTILINESTRING", "MULTILINESTRING ZM")

    return multilineWKT4d


def TokenizeString(aString, separators):
    print("start TokenizeString")
    # separators is an array of strings that are being used to split the string.
    # sort separators in order of descending length
    separators.sort(key=len)
    allSeparated = []
    i = 0
    while i < len(aString):
        theSeparator = ""
        for current in separators:
            if current == aString[i:i + len(current)]:
                theSeparator = current
        if theSeparator != "":
            allSeparated += [theSeparator]
            i = i + len(theSeparator)
        else:
            if allSeparated == []:
                allSeparated = [""]
            if (allSeparated[-1] in separators):
                allSeparated += [""]
            allSeparated[-1] += aString[i]
            i += 1
    ##    print allSeparated

    separatedWithSeparatorList = []
    separatedValue = ""
    for i in (range(len(allSeparated))):
        if allSeparated[i] in ("POINT", "MULTILINESTRING"):
            separatedValue = allSeparated[i]
        ##            print separatedValue
        elif separatedValue != "":
            separatedValue = separatedValue + allSeparated[i].rstrip(" ").rstrip(",")
            separatedWithSeparatorList.append(separatedValue)
    ##    print separatedWithSeparatorList
    print("separatedWithSeparatorList: %s" % separatedWithSeparatorList)
    return separatedWithSeparatorList


def JoinMultiLinesToMultiline(multiLines):
    ##    print "start JoinMultiLinesToMultiline"
    ##    print "multiLines = %s" %multiLines
    multiLineList = []
    for multiLine in multiLines:
        print("multiLine = %s" % multiLine)
        multiLineString = multiLine.split("MULTILINESTRING(")[1][:-1]
        seperator = "),"
        print("multiLineString = %s" % multiLineString)
        lineList = ([e.strip(")") + ")" for e in multiLineString.split(seperator) if e])
        for line in lineList:
            print("line = %s" % line)
            multiLineList.append(line)
    multiLineStringNew = "MULTILINESTRING(%s)" % (",".join(multiLineList))
    ##    print "multiLineStringNew",multiLineStringNew

    return multiLineStringNew


def GeometrycollectionToMultiLine(geometrycollection):
    ##    print ("\nstart GeometrycollectionToMultiLine")
    geometrieCollectionString = geometrycollection.split("GEOMETRYCOLLECTION(")[-1][:-1]
    ##    print (geometrieCollectionString[:200],"......",geometrieCollectionString[-5:])
    separators = ["MULTILINESTRING", "POINT"]
    geometrieCollectionList = TokenizeString(geometrieCollectionString, separators)
    ##    print ("\n")
    ##    print ("geometrieCollectionList: %s" %geometrieCollectionList)

    for i in range(len(geometrieCollectionList)):
        if "POINT" in geometrieCollectionList[i]:
            ##            print ("POINT in geometrieCollection")
            geometrieCollectionList[i] = pointWktToLineWkt(geometrieCollectionList[i])
        print("geometrieCollectionList", geometrieCollectionList)
    multiLineString = JoinMultiLinesToMultiline(geometrieCollectionList)
    ##    print ("\n")
    ##    print ("multiLineString",multiLineString)

    return multiLineString


def mExtendToAttribute(routeLayer):
    print("maak velden aan met eigenschappen geometrie")
    if arcpy.GetCount_management(routeLayer) > 0 and arcpy.ListFields(routeLayer, "ident8"):
        for field in ('MinM', 'MaxM'):
            arcpy.AddField_management(routeLayer, field, "DOUBLE", "#", "#", "#", "#", "#", "#", "#")
            if field == 'MinM':
                arcpy.CalculateField_management(routeLayer, field, '(!Shape.extent.MMin!)', 'PYTHON_9.3')
            else:
                arcpy.CalculateField_management(routeLayer, field, '(!Shape.extent.MMax!)', 'PYTHON_9.3')
    fields = arcpy.ListFields(routeLayer)
    for field in fields:
        print(field.name, field.type)

    print("geometrie eigenschappen aangemaakt")


def pointWktToLineWkt(pointWkt):
    import sys
    pointCoordinateList = pointWkt.split("(")[-1].strip(")").split(" ")
    if len(pointCoordinateList) == 2:
        lineWkt2d = "MULTILINESTRING((%s %s, %s %s))" % (
            pointCoordinateList[0], pointCoordinateList[1], pointCoordinateList[0], pointCoordinateList[1])
        return lineWkt2d
    elif len(pointCoordinateList) == 4:
        lineWkt4d = "MULTILINESTRING((%s %s %s %s, %s %s %s %s))" % (
            pointCoordinateList[0], pointCoordinateList[1], pointCoordinateList[2], pointCoordinateList[3],
            pointCoordinateList[0], pointCoordinateList[1], pointCoordinateList[2], pointCoordinateList[3])
        return lineWkt4d
    else:
        print("error pointWkt")
        sys.exit()


def MaakElement(row, ListFieldName, DictFieldNameFClass, teller_rij, csvFile, dataType):
    ##    print ("row voor MaakElement : %s" %row)
    # voeg voorlopige ontbrekende waarden toe
    copyDatum = ''
    deltaWijzigingsDatum = ''
    if "wijzigingsdatum" in ListFieldName or "laatsteUpdate" in ListFieldName:
        row.extend((copyDatum, deltaWijzigingsDatum))
    else:
        row.append((copyDatum))

    newRow = []
    teller_veld = 0

    for FieldName in ListFieldName:
        ##        print "teller_veld = %s" %teller_veld
        ##        print ("FieldName = %s, Waarde = %s" %(FieldName, (row[teller_veld])[0:250]))
        if FieldName in DictFieldNameFClass and FieldName not in ('shape', 'geometry'):
            ##            print ("FieldName = %s, Waarde = %s" %(FieldName, (row[teller_veld])[0:250]))
            FieldType = DictFieldNameFClass.get(FieldName, "none")
            try:
                if FieldName not in ("omschrijving",):
                    ##                    print "FieldName '%s' not in ('omschrijving',), waarde wordt begrensd op 250 karakters" % (FieldName)
                    Waarde = (row[teller_veld])[0:250]
                else:
                    ##                    print "FieldName '%s' in ('omschrijving',), waarde wordt begrensd op 250 karakters" % (FieldName)
                    Waarde = (row[teller_veld])
            ##                    print "%s in omschrijving" %Waarde
            ##                print ("Waarde %s = %s" % (FieldName,Waarde) )
            except:
                print("*** Waarde kan niet berekend worden voor FieldName '%s' met FieldType '%s'in rij '%s'" % (
                    FieldName, FieldType, teller_rij))

            if FieldType == 'Date':
                if FieldName == 'CopyDatum':
                    Waarde = datetime.now()
                else:
                    Waarde = Waarde.split("T")[0]
                    try:
                        Waarde = datetime.strptime(Waarde, '%Y-%m-%d')
                    except:
                        try:
                            Waarde = datetime.strptime(Waarde, '%d/%m/%Y')
                        except:
                            Waarde = datetime.strptime('1950-01-01', '%Y-%m-%d')
                    # onthoudt wijzigingsdatum om te later te gebruiken
                    if FieldName in ('wijzigingsdatum', 'laatsteUpdate'):
                        WijzigingsDatum = Waarde

            elif FieldName == 'DeltaWijzigingsDatum':
                Waarde = ((datetime.today() - WijzigingsDatum).days)
                try:
                    Waarde = ((datetime.today() - WijzigingsDatum).days) / 365.00
                    ##	print 'Deltawijzigingsdatum', Waarde, 'Wijzigingsdatum', WijzigingsDatum
                except:
                    Waarde = 0

            elif FieldType in ('Long', 'Double'):
                try:
                    Waarde = float(Waarde)
                    ## print 'float'
                except:
                    ## print'error', FieldName,FieldType, Waarde
                    Waarde = 0

            newRow.append(Waarde)

        elif FieldName == 'geometry':
            ##            print "voeg geometry toe"
            geometry = row[teller_veld].replace("SRID=31370;", "").replace(",", ", ")
            if "GEOMETRYCOLLECTION EMPTY" in geometry:
                geometry = "SRID=31370;POINT(0 0 0 0)"
            if "POINT" in geometry and csvFile in (
                    'straatkolken', 'referentiepunten', 'referentiepunten2', 'bomen', 'vluchtdeurensassen'):
                geometry = Geometry4dTo2D(geometry)

            elif "GEOMETRYCOLLECTION" in geometry:
                geometry = GeometrycollectionToMultiLine(geometry)

            elif csvFile in ('innames', 'knelpunten_locaties'):
                if "POINT" in geometry:
                    geometry = pointWktToLineWkt(geometry)
                else:
                    geometry = "MULTILINESTRING ZM " + "(" + geometry.split("(", 1)[1]

                geometry = Wkt2DTo4D(geometry)


            elif csvFile == 'GenummerdeWegenWdb':
                geometry = "MULTILINESTRING ZM " + "(" + geometry.split("(", 1)[1]
        ##                geometry = row[teller_veld].replace("SRID=31370;MULTILINESTRING","MULTILINESTRING ZM ").replace("SRID=31370;LINESTRING","MULTILINESTRING ZM ").replace("LINESTRING","MULTILINESTRING ZM ").replace(",",", ")
        teller_veld += 1
    if dataType != "table":
        newRow.append(geometry)

    return newRow


def MakeFeatureClassFromCsv(csvFile, in_map, WsMem, dataType):
    print("maak feature class van %s" % csvFile)
    import os, csv, arcpy, sys
    ##    csv.field_size_limit(500 * 1024 * 1024)
    csv.field_size_limit(sys.maxsize)
    in_csvFile = os.path.join(in_map, csvFile + ".csv")
    out_map_table = os.path.join(WsMem, csvFile)
    csvFile_reader = csv.reader(open(in_csvFile, 'r'), delimiter='|')
    teller_veld = 0
    teller_rij = 0.0
    ##
    ##    row_count = sum(1 for row in csvFile_reader)
    ##    print ("row_count = %s" % row_count)
    for row in csvFile_reader:
        if len(row) < 2:
            print("len(row) < 10, row = %s" % row)
            break
        ##        print ("teller_rij = %s" %teller_rij)

        ##		print row
        # beperking in het aantal rijen die gelezen worden
        max_rijen = 0
        if teller_rij > max_rijen and max_rijen > 0:
            print("break - max %s rijen" % max_rijen)
            break

        # aantal velden bepalen
        if teller_rij == 0.0:
            # tabel aanmaken
            ListFieldNameFClass, DictFieldNameFClass, ListFieldName = MaakTabel(row, WsMem, csvFile, out_map_table,
                                                                                dataType)

        if teller_rij == 0.0:  ##or teller_rij/50000 == int(teller_rij/50000)
            ####            print "hernieuw cursor"
            ##            if "CurOutTable" in locals():
            ##                del CurOutTable
            ####                print "oude cursor verwijderd"
            CurOutTable = arcpy.da.InsertCursor(out_map_table, ListFieldNameFClass)

        # data van csv naar shp
        if teller_rij > 0:
            if teller_rij / 10000 == int(teller_rij / 10000):
                from datetime import datetime
                print(datetime.now())
                print("rij %s is ingelezen" % teller_rij)

            newRow = MaakElement(row, ListFieldName, DictFieldNameFClass, teller_rij, csvFile, dataType)

            ##            print ("velden: %s" % [f.name for f in arcpy.ListFields(out_map_table)])
            ##            print ("newRow = %s" %newRow)
            ##            print ("ListFieldNameFClass : %s" %ListFieldNameFClass)
            try:
                ##                print ("voeg rij toe voor newRow = %s" %newRow)
                CurOutTable.insertRow(newRow)
            except:
                print("toevoegen rij is mislukt voor newRow = %s" % newRow)

        teller_rij += 1
    ##    print 'GetCount',arcpy.GetCount_management(out_map_table)
    print("einde MakeFeatureClassFromCsv")
    return str(out_map_table)


def CreateOffsetVelden(out_map_table):
    print("maak offsetvelden aan")
    if arcpy.GetCount_management(out_map_table) > 0 and arcpy.ListFields(out_map_table, "ident8"):
        arcpy.AddField_management(out_map_table, "offset", "LONG", "#", "#", "#", "#", "#", "#", "#")
        arcpy.MakeTableView_management(out_map_table, "tableview")
        arcpy.CalculateField_management("tableview", "offset", 40, 'PYTHON_9.3')
        whereclause = """ "ident8" LIKE '%1' """
        arcpy.SelectLayerByAttribute_management("tableview", "NEW_SELECTION", whereclause)
        arcpy.CalculateField_management("tableview", "offset", -40, 'PYTHON_9.3')
        arcpy.SelectLayerByAttribute_management("tableview", "CLEAR_SELECTION", "#")


##	print"offsetvelden aangemaakt"


def CorrigeerLengte(csvFile):
    print("Corrigeer lengte")
    print("veldnamen : %s" % [f.name for f in arcpy.ListFields(csvFile)])
    arcpy.CalculateField_management(csvFile, field="lengte", expression="([eindpositie] - [beginpositie])*1000",
                                    expression_type="VB", code_block="")


def JsonGeometryToEsriWkt(geometry):
    x = geometry["coordinates"][0]
    y = geometry["coordinates"][1]
    esriWkt = "POINT (%s %s)" % (x, y)
    return esriWkt


def ConvertStringToDate(value):
    from datetime import datetime
    if value is None or value == "":
        value = "1950-01-01"
    value = value.split("T")[0]
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
            print('no valid date format found')
            return "1950-01-01"
    return value


def ConvertDataType(value, fieldType, fieldLength):
    if fieldType in ("Date",):
        value = ConvertStringToDate(value)
    elif value == None:
        value = None
    elif value in (False, True):
        value = str(value)
    elif fieldType == "String":
        if type(value) in (list, tuple):
            value = ";".join(value)
        try:
            value = unicode(value).encode('utf-8')[:fieldLength]
        except:
            print("kan niet gelezen worden als string")
            value = "fout bij inlezen json-fs"
    elif fieldType in ("Double", "Float"):
        value = float(value)

    return value


def JsonLineToFeature(jsonLine, icFields, icFieldProperties, jsonToFeatureLink):
    from datetime import datetime
    # "[u'OBJECTID', u'SHAPE', u'ident8', u'gebied', u'gekozenbeheerder', u'begindatum', u'actueleOpstelling', u'berekendeBeheerder', u'id']"
    row = []
    for f in icFields:
        if f in ("OBJECTID", "DeltaWijzigingsdatum"):
            value = None
        elif f in ("CopyDatum", "copyDatum", "copydatum"):
            value = datetime.now()
        elif f in ("DeltaWijzigingsDatum"):
            value = ""  # wordt als laatste ingevuld
        elif len(jsonToFeatureLink[f]) == 1:
            if jsonToFeatureLink[f][0] == "geometry":
                if jsonLine["geometry"]["type"] == "Point":
                    value = arcpy.AsShape(jsonLine['geometry'])
                elif jsonLine["geometry"]["type"] in ("MultiLineString", "LineString"):
                    value = arcpy.AsShape(jsonLine['geometry'])
                else:
                    print('FOUT: geometry kan niet worden verwerkt (JsonLineToFeature)')
            else:
                value = ConvertDataType(jsonLine[jsonToFeatureLink[f][0]], icFieldProperties[f][0],
                                        icFieldProperties[f][1])

        elif jsonToFeatureLink[f][0] == "properties":
            if jsonToFeatureLink[f][1] in jsonLine["properties"]:
                value = ConvertDataType(jsonLine["properties"][jsonToFeatureLink[f][1]], icFieldProperties[f][0],
                                        icFieldProperties[f][1])
            else:
                ##                print("key %s bestaat niet" %(jsonToFeatureLink[f][1]))
                value = -8
        else:
            print('FOUT')

        if value == "nvt" and icFieldProperties[f][0] != "String":
            value = -8
        ##        arcpy.AddMessage("value: %s: %s" %(f,value))
        row.append(value)

    # voeg deltawijzigingsdatum toe
    if "DeltaWijzigingsDatum" in icFields:
        wijzigingsdatum = row[icFields.index("wijzigingsdatum")]
        if wijzigingsdatum != None:
            row[icFields.index("DeltaWijzigingsDatum")] = (datetime.today() - wijzigingsdatum).days / 365.00

    return row

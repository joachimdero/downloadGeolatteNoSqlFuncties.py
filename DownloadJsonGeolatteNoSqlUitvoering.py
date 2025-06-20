import logging
import os, importlib
from sys import path


import downloadGeolatteNoSqlFuncties_py3

# importeer Awv functie en downloadFunctie
path_awvfuncties = r"C:\TeamAssetBeheer\awvDataSync\AwvFuncties"
path.append(path_awvfuncties)
import AwvFuncties
import AuthenticatieProxyAcmAwv

importlib.reload(AwvFuncties)
importlib.reload(downloadGeolatteNoSqlFuncties_py3)

# parameters
folder_out = r"C:\TeamAssetBeheer\awvDataSync\prod\awvData\downloadFeatureServer"
log = r"C:\TeamAssetBeheer\awvDataSync\prod\awvDataTools\log\log_downloadJson.log"
awv_key = r"C:\TeamAssetBeheer\awvDataSync\Awv\awv.json"

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Stel het laagste niveau in (bijv. DEBUG), zodat alles wordt gelogd
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log),  # Log naar een bestand
        logging.StreamHandler()  # Log naar de console
    ]
)
logging.info("start LOG")

# Omgeving
omgeving = "productie"  # Kies uit: "productie", "dev", "tei"
omgeving_url = f"apps-{omgeving}" if omgeving in {"dev", "tei"} else "apps"

base_url = f"https://services.{omgeving_url}.mow.vlaanderen.be/geolatte-nosqlfs/cert/api/databases"
url_fs = f"{base_url}/featureserver/"
geolatte_nosqlfs_published = f"{base_url}/published/"
geolatte_nosqlfs_eminfra = f"{base_url}/eminfra/"


# ////////////////////////////////////////////////////////////////////////////////////////////////////////

# lagen
lagenDict = {
    'verkeersborden': [
        "properties.id",
        "properties.wegsegmentid",
        "properties.straat",
        "properties.ident8",
        "properties.opschrift",
        "properties.afstand",
        "properties.langsGewestweg",
        "properties.zijdeVanDeRijweg",
        "properties.gemeente",
        "properties.actueleOpstelling",
        "properties.beheerder.gebiednummernaam",
        "properties.status",
        "properties.beginDatum",
        "properties.wijzigingsDatum",
        "properties.aanzichten[id,clientId,hoek,type,wegsegmentid,wegsegmentidGok,wijzigingsTimestamp,borden[id,code,breedte,hoogte,vorm,folieType,datumPlaatsing,clientId,actief,beheerder,parameters,leverancierItem.naam,fabricageJaar,fabricageType.naam,besteknummer]]"
    ],

    "referentiepunten2": [
        "id",
        "properties.wegnummer",
        "properties.opschrift",
        "properties.bijkomendewegnummers",
        "properties.materiaalPaal",
        "properties.bijkomendewegnummers",
        "properties.opmerking",
        "properties.gebruiker",
        "properties.begindatum",
        "properties.opnamedatum",
        "properties.creatiedatum",
        "properties.wijzigingsdatum",
    ],
    'wegenregister': [
        "properties"
    ],
    'genummerd_wegenregister': [
        "properties"
    ],
    '9bd29f88-da6f-4ed7-98d4-b664b5486b31': [
        "properties"
    ],
    'f309a658-d3f0-4cba-826f-eda3340d4650': [
        "properties"
    ],
    'projectcenter': [
        "properties"
    ],
    "knelpunten_locaties": [
        "properties"
    ],
    "fietspaden_wrapp": [
        "properties"
    ],
    "fietssuggestiestroken_wrapp": [
        "properties"
    ],
    "snelheidsregimes_wrapp": [
        "properties"
    ],
    "lzv_trajecten_wrapp": [
        "properties"
    ],
    "bebouwdekommen_wrapp": [
        "properties"
    ],
    "fastpercelenplus35t_wrapp": [
        "properties"
    ],

    "innames": [
        "properties"
    ],
    "afgeleide_snelheidsregimes_wegsegment_bord": [
        "properties"
    ],
    "afgeleide_tonnage_voorwaarden": [
        "properties"
    ],
    "afgeleide_tonnage": [
        "properties"
    ],
    "afgeleide_zones": [
        "properties"
    ],
    "uvroutes": [
        "properties"
    ],
    "emonderdelen": [
        "properties"
    ],
    "ingrepen": [
        "properties"
    ],
    "currentstaatvandewegmetingen_V2": [
        "properties"
    ],
    "staatvandewegmetingen_V2": [
        "properties"
    ],
}
lagen = [
    'innames',
    'afgeleide_snelheidsregimes_wegsegment_bord',
    'afgeleide_tonnage_voorwaarden',
    'afgeleide_tonnage',
    'afgeleide_zones',
    'currentstaatvandewegmetingen_V2',
    'staatvandewegmetingen_V2',
    "bebouwdekommen_wrapp",
    'emonderdelen',
    "fastpercelenplus35t_wrapp",
    "fietspaden_wrapp",
    'uvroutes',
    'knelpunten_locaties',
    "fietssuggestiestroken_wrapp",
    "ingrepen",
    "knelpunten_locaties",
    "lzv_trajecten_wrapp",
    "projectcenter",
    "referentiepunten2",
    "snelheidsregimes_wrapp",
    "verkeersborden",
    "wegenregister",
    'genummerd_wegenregister',
    'f309a658-d3f0-4cba-826f-eda3340d4650'
]

session = AuthenticatieProxyAcmAwv.OAuthSession(awv_key)

for laag in lagen:
    logging.info(f"download laag {laag}")
    out_json_basename = laag + ".json"
    url = url_fs
    if laag == "referentiepunten2":
        out_json_basename = "referentiepunten.json"
    elif laag == "emonderdelen":
        url = geolatte_nosqlfs_eminfra
    elif laag == "f309a658-d3f0-4cba-826f-eda3340d4650":
        url = geolatte_nosqlfs_published
        out_json_basename = ("UitzonderlijkVervoerNetwerk.json")
    out_json = os.path.join(folder_out, out_json_basename)

    projection = ",".join(lagenDict[laag])


    try:
        downloaded_size = downloadGeolatteNoSqlFuncties_py3.download_json_stream_new(
            laag=laag,
            out_json=out_json,
            url_basis=url,
            projection=projection,
            session=session
        )

    except Exception as e:
        print("ERROR")
        logging.error(e)

logging.info("stop LOG")

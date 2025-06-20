import json
import os
import logging
import traceback
from datetime import datetime
import pandas as pd
import geopandas as gpd

from Geojsonl_to_gpkg_gdb_functies import make_df, read_jsonlines

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Stel het laagste niveau in (bijv. DEBUG), zodat alles wordt gelogd
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(r"C:\TeamAssetBeheer\awvDataSync\prod\awvDataTools\log\log_json_to_gpkg_gdb.log"),
        # Log naar een bestand
        logging.StreamHandler()  # Log naar de console
    ]
)
logging.info("start LOG")

out_format_json = r"C:\TeamAssetBeheer\awvDataSync\prod\awvDataTools\json_to_gisformat\out_format.json"
with open(out_format_json, 'r') as file:
    out_format = json.load(file)

in_jsonl_path = r"C:\TeamAssetBeheer\awvDataSync\prod\awvData\downloadFeatureServer"

i = 0  # vervangen door enumerate
for i,laag in enumerate(out_format["lagen"]):
    logging.info(f"\nstart verwerking laag {i}.{laag}")
    if laag not in (
            'afgeleide_tonnage_voorwaarden',
            'afgeleide_zones',
            'afgeleide_snelheidsregimes_wegsegment_bord',
            'afgeleide_tonnage',
            'bebouwdekommen_wrapp',
            'emonderdelen',
            'fastpercelenplus35t_wrapp',
            'fietspaden_wrapp',
            'fietssuggestiestroken_wrapp',
            'genummerd_wegenregister',
            'ingrepen',
            'innames',
            'knelpunten_locaties',
            'lzv_trajecten_wrapp',
            'projectcenter',
            'referentiepunten',
            'snelheidsregimes_wrapp',
            'staatvandewegmetingen_V2',
            'wegenregister',
    ):#e
        logging.info("laag niet in lijst")
        continue

    try:
        in_jsonl = os.path.join(in_jsonl_path, laag + ".json")
        df, f_names = make_df(in_jsonl, out_format, laag)

        # Voeg data toe aan het bestaande DataFrame
        new_data = pd.DataFrame(read_jsonlines(in_jsonl, f_names, out_format, laag))

        # Gebruik pd.concat om de nieuwe data toe te voegen aan het bestaande DataFrame
        df = pd.concat([df, new_data], ignore_index=True)
        # Dwing int32 af voor alle relevante kolommen
        int_cols = [col for col, info in out_format["lagen"][laag]["velden"].items() if info["veldtype"] == "int32"]
        for int_col in int_cols:
            try:
                df[int_col] = df[int_col].astype("int32")
            except:
                logging.error(f"probleem: int cols: {int_col}")
                logging.error(f"Voorbeeld foutieve waarden in kolom '{int_col}':")
                logging.error(df[int_col][~df[int_col].apply(lambda x: isinstance(x, int) or str(x).isdigit())].head())
                pass

        # Zet de 'copydatum' kolom om naar een datetime-object
        df['copydatum'] = pd.to_datetime(df['copydatum'], errors='coerce')  # TODO: verplaatsen

        # Zet het DataFrame om naar een GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=df['geometry'], crs=31370)
        if gdf.geometry.geom_type.isin(['LineString', 'MultiLineString']).any():
            gdf['SHAPE_Length'] = gdf.geometry.length

        # Controleer het resultaat
        logging.info(f"Aantal rijen in het DataFrame: {gdf.shape[0]}")

        # Exporteer het GeoDataFrame naar een GeoPackage
        logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  maak geopackage")
        gdf.to_file(r"C:\TeamAssetBeheer\awvDataSync\prod\awvData\AwvData.gpkg", layer=laag, driver="GPKG")
        logging.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  maak OpenFileGDB")
        # Schrijven naar OpenFileGDB

        gdf.to_file(r"C:\TeamAssetBeheer\awvDataSync\prod\awvData\AwvData.gdb",
                    layer=laag,
                    driver="OpenFileGDB"
                    )
        # geojson toevoegen

    except FileNotFoundError as e:
        logging.error(f"Bestand niet gevonden voor laag {laag}: {e}")
    except ValueError as e:
        logging.error(f"Waarde fout bij het verwerken van laag {laag}: {e}")
    except Exception as e:
        logging.error(f"Onverwachte fout bij het verwerken van laag {laag}: {e}")
        print(traceback.format_exc())

logging.info("stop LOG")

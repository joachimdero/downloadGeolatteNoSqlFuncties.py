import json
import sys
from datetime import datetime

import pandas as pd
from shapely.geometry import shape


def make_df(in_jsonl, out_format:dict, laag:str)->(pd.DataFrame,list):
    # Haal de veldnamen en veldtypen op
    f_names = out_format["lagen"][laag]["velden"].keys()

    # Maak een Pandas DataFrame
    df = pd.DataFrame(data=None, columns=f_names)

    # Zet de datatypes aan de hand van de veldtypen uit de dictionary
    for f in f_names:
        f_type = out_format["lagen"][laag]["velden"][f]["veldtype"]
        if f_type == "int":
            df[f] = df[f].astype("int32")
        elif f_type == "decimal":
            df[f] = df[f].astype('float64')
        elif f_type == "str":
            df[f] = df[f].astype(str)
        elif f_type == "date":
            df[f] = pd.to_datetime(df[f], format='%Y-%m-%d')

    return df, f_names



def extract_nested_value(data, path):
    """Haalt de geneste waarde uit een dictionary of lijst op basis van het pad."""
    keys = path.split(".")
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
        elif isinstance(value, list):
            try:
                index = int(key)  # Probeer de sleutel als index te interpreteren
                value = value[index]
            except (ValueError, IndexError):
                return None  # Ongeldige index of index buiten bereik
        else:
            return None  # Huidige waarde is geen dict of lijst
    return value


def read_jsonlines(file_path, f_names, out_format, laag):
    with open(file_path, 'r', encoding='utf-8') as file:
        for i,line in enumerate(file):
            if i % 10000 == 0 or i in (10,20,50,100,200,500,1000,2000,5000):
                sys.stdout.write(f"\rread jsonlines: {i} rows")
                sys.stdout.flush()
            if i >= 1000:
                pass

            data = json.loads(line)  # Parse de JSON-lijn
            properties = data.get('properties', {})  # Haal de properties op
            relevant_data = {}

            for field in f_names:
                # Kijk of er een 'source' is opgegeven in out_format
                field_info = out_format["lagen"][laag]["velden"][field]
                source = field_info.get("source", field)  # Gebruik 'source' of het veld zelf als sleutel

                # Als 'source' een genest pad bevat, gebruik extract_nested_value
                if "." in source:
                    relevant_data[field] = extract_nested_value(properties, source)
                else:
                    relevant_data[field] = properties.get(source)

                # Controleer of het veldtype een datum is en converteer deze
                if field_info["veldtype"] == "date" and relevant_data[field] is not None:
                    relevant_data[field] = parse_date(relevant_data[field])
                elif field_info["veldtype"] == "int32" and relevant_data[field] is None:
                    relevant_data[field] = 0

            geometry_type_laag = out_format["lagen"][laag].get("geometry_type", None)
            geometry, richting = process_geometry(data['geometry'], geometry_type_laag=geometry_type_laag)

            # Zet de geometrie om naar een Shapely object
            geometry = shape(geometry)
            relevant_data['geometry'] = geometry  # Voeg de geometrie toe
            if richting is not None:
                relevant_data['richting'] = richting
            relevant_data['copydatum'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
            relevant_data['id'] = data['id']

            yield relevant_data


def remove_m_from_coordinates(coordinates, geometry_type):
    """Remove m-value if present, based on the structure of the first coordinate."""
    if geometry_type in ("Point", "POINT"):
        if len(coordinates) == 4:
            return coordinates[:3]
        elif len(coordinates) == 2:
            return coordinates[:2] + [0]
        else:
            return coordinates
    elif geometry_type in ("MultiLineString", "LineString"):
        if len(coordinates[0]) == 4:  # x, y, z, m
            return [[x, y, z] for x, y, z, _ in coordinates]
        elif len(coordinates[0]) == 2:  # x, y, z, m
            return [[x, y, 0] for x, y in coordinates]
        elif len(coordinates[0]) < 4:  # x, y, z
            return coordinates
        else:
            raise ValueError("Unexpected coordinate length")


def point_to_multilinestring(coordinates):
    coordinates = [[[coordinates[0], coordinates[1], coordinates[2]], [coordinates[0], coordinates[1], coordinates[2]]]]
    return coordinates


def geometrycollection_to_multilinestring(geometry_dict):
    if geometry_dict['geometries'] == []:
        return [[[0, 0], [0, 0]]]

    all_lines = []

    # Recursieve helperfunctie om door de geometrieën te itereren
    def process_geometry(geom):
        geometry_type = geom['type']

        if geometry_type == 'GeometryCollection':
            for sub_geom in geom['geometries']:
                process_geometry(sub_geom)  # Roep de functie opnieuw aan voor geneste geometrieën
        else:
            coordinates = geom['coordinates']
            if geometry_type == 'Point':
                new_coordinates = remove_m_from_coordinates(coordinates, geometry_type)
                # Converteer een punt naar een LineString met twee identieke coördinaten
                all_lines.append([new_coordinates, new_coordinates])
            elif geometry_type == 'LineString':
                new_coordinates = remove_m_from_coordinates(coordinates, geometry_type)
                # Voeg LineString toe aan de lijst
                all_lines.append(new_coordinates)
            elif geometry_type == 'MultiLineString':
                new_coordinates = [remove_m_from_coordinates(line, geometry_type) for line in coordinates]
                # Voeg alle lijnen van MultiLineString toe aan de lijst
                all_lines.extend(new_coordinates)

    # Itereer door de hoofdgeometrieën in de GeometryCollection
    for geom in geometry_dict['geometries']:
        process_geometry(geom)

    # Maak een nieuwe MultiLineString GeoJSON
    geometry_dict['type'] = 'MultiLineString'
    geometry_dict['coordinates'] = all_lines
    return all_lines


def z_geometrycollection_to_multilinestring(geometry_dict):
    if geometry_dict['geometries'] == []:
        return [[[0, 0], [0, 0]]]

    all_lines = []
    # Itereer door de geometrieën in de GeometryCollection
    for geom in geometry_dict['geometries']:
        print(f"geometry_dict {geometry_dict}")
        print(f"geometry_dict['geometries'] {geometry_dict['geometries']}")
        print(f"geom {geom}")
        geometry_type = geom['type']
        coordinates = geom['coordinates']
        if geometry_type == 'Point':
            new_coordinates = remove_m_from_coordinates(coordinates, geometry_type)
            # Converteer een punt naar een LineString met twee identieke coördinaten
            all_lines.append([new_coordinates, new_coordinates])
        elif geometry_type == 'LineString':
            new_coordinates = remove_m_from_coordinates(coordinates, geometry_type)
            # Voeg LineString toe aan de lijst
            all_lines.append(new_coordinates)
        elif geometry_type == 'MultiLineString':
            new_coordinates = [remove_m_from_coordinates(line, geometry_type) for line in coordinates]
            # Voeg alle lijnen van MultiLineString toe aan de lijst
            all_lines.extend(new_coordinates)

    # Maak een nieuwe MultiLineString GeoJSON
    geometry_dict['type'] = 'MultiLineString'
    geometry_dict['coordinates'] = all_lines
    return all_lines


def process_richting(coordinates):
    coordinate_start, coordinate_end = coordinates[0], coordinates[-1]
    if len(coordinate_start) < 4 or len(coordinate_end) < 4:
        return None
    else:
        if coordinate_start[3] < coordinate_end[3]:
            return 1
        elif coordinate_start[3] > coordinate_end[3]:
            return 2
        else:
            return None



def process_geometry(geometry_dict, geometry_type_laag=None):
    """Process geometry dictionary to remove m-value where necessary."""
    geometry_type = geometry_dict['type']
    richting = None

    if geometry_type in ('GeometryCollection',):
        new_coordinates = geometrycollection_to_multilinestring(geometry_dict)
    else:
        coordinates = geometry_dict['coordinates']
        if coordinates == []:
            new_coordinates = [[[0, 0], [0, 0]]]
        elif geometry_type in ('Point',):
            new_coordinates = remove_m_from_coordinates(coordinates, geometry_type)
        elif geometry_type in ('LineString',):
            richting = process_richting(coordinates)
            geometry_type_laag = 'MultiLineString'
            new_coordinates = [remove_m_from_coordinates(coordinates, geometry_type)]
        elif geometry_type in ('MultiLineString', 'MULTILINESTRING'):
            new_coordinates = [remove_m_from_coordinates(line, geometry_type) for line in coordinates]
        else:
            raise ValueError(f"Unsupported geometry type:{geometry_type}")

    if geometry_type_laag == "MULTILINESTRING" and geometry_type in ("Point", "POINT"):
        new_coordinates = point_to_multilinestring(new_coordinates)
    geometry_dict['coordinates'] = new_coordinates
    if geometry_type_laag is not None:
        geometry_dict['type'] = geometry_type_laag
    if len(str(new_coordinates)) < 10:
        print(f"voor exit:{new_coordinates}")
        print(f"voor exit:{geometry_dict}")
        sys.exit()

    return geometry_dict, richting

def parse_date(datum_str):
    # print(datum_str)
    # Lijst van veelvoorkomende datumformaten
    date_formats = (
        '%d-%m-%Y',          # 01-06-2023
        '%Y-%m-%dT%H:%M:%S', # 2023-06-01T12:30:45
        '%d/%m/%Y',          # 01/06/2023
        '%Y-%m-%d',          # 2023-06-01
        '%m/%d/%Y',          # 06/01/2023
        '%Y/%m/%d'           # 2023/06/01
    )

    for date_format in date_formats:
        try:
            # Probeer elk formaat
            return pd.to_datetime(datum_str, format=date_format, errors='raise')
        except ValueError:
            continue  # Ga door naar het volgende formaat
            # return pd.to_datetime(datum_str, format=date_format, errors='raise')

    # Als geen enkel formaat werkt, retourneer NaT
    return pd.NaT
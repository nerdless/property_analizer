import pandas as pd
from os.path import exists
import json
from json import JSONDecodeError
import os
import sqlite3
import numpy as np

from openai import OpenAI
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

crawling_date = "2024-05-28"
input_file = "prop_data-2024-05-28.json"
db_file = "prop_data.db"
data_pickle = "prop_data.pkl"


contacted = [
    "+529851124723",
    "+529997490467",
    "+529991384600",
    "+529993350294",
    "+529992359443",
    "+529992505514",
    "9993094963",
    "+529991392940",
    "+529994538999",
    "+529999551484",
    "+529993709158",
    "+529993491711",
    "9851001222",
    "9851096721",
    "+529993383135",
    "+529221267836",
    "+529999900127",
    "+52999 994 3786",
    "+529999935424",
    "+525568278382",
    "+529992505514"
]

municipalities_yucatan = [
    "Celestún",
    "Umán",
    "Chocholá",
    "Maxcanú",  #
    "Kopomá",  #
    "Opichén",
    "Abalá",
    "Muna",
    "Chapab",
    "Sacalum",
    "Tecoh",
    "Titee",
    "Cuzamá",
    "Homún",
    "Huhí",
    "Kantunil",
    "Izamal",
    "Dzoncauich",
    "Tekal de Venegas",
    "Quintana Roo",
    "Sotuta",
    "Sudzal",
    "Tunkás",
    "Cenotillo",
    "Buctzotz",
    "Dzilam González",
    "Dzilam de Bravo",
]


class PropertyRepo:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS properties
             (url text, price real, currency text, plot_area real, build_area real, rooms integer, bathrooms integer, publication_date text, amenities text)''')
        self.conn.commit()

    def insert(self, url, price, currency, plot_area, build_area, rooms, bathrooms, publication_date, amenities):
        self.cursor.execute("INSERT INTO properties VALUES (?,?,?,?,?,?,?,?,?)", (url, price, currency, plot_area, build_area, rooms, bathrooms, publication_date, amenities))
        self.conn.commit()

    def close(self):
        self.conn.close()

    def get(self):
        self.cursor.execute("SELECT * FROM properties")
        return self.cursor.fetchall()

    def exists(self, url):
        self.cursor.execute("SELECT * FROM properties WHERE url = ?", (url,))
        return self.cursor.fetchone() is not None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


def clean_price(price):
    price = price.strip()
    if price.startswith("USD"):
        price = float(price[3:].replace('$', '').replace(",","").strip())
        return price, "USD"
    elif price.endswith("MXN"):
        price = float(price[:-3].replace('$', '').replace(",","").strip())
        return price, "MXN"
    else:
        return None, None

def clean_area(area):
    if pd.notnull(area) and area.endswith("m²"):
        area = float(area.strip()[:-2].replace(",","").strip())
        return area
    else:
        return None

property_repo = PropertyRepo(db_file)

HOUSE_PROMPT = "You are helpful assistant, you take a house description and return a JSON with parsed data about the house. The data should include the following fields: price, currency, plot_area, price_per_square_meter, build_area, rooms, bathrooms, publication_date, remate and easybroker_id. The data should be cleaned and normalized. The price, plot_area, price_per_square_meter and build_area should be floats. The publication_date should be a datetime object. The easybroker_id is a string. remate field is a boolean saying if the property is a remate bancario or not. If there is no information, or you are not sure, do not include the field in the JSON."

PROMPT_TERRENO = "Eres un asistente útil, tomas una descripción de un terreno y devuelves un JSON con datos normalizados sobre el terreno. Los datos deben incluir los siguientes campos: precio, moneda, superficie total, precio por metro cuadrado (M2) y fecha de publicación. Los datos deben estar limpios y normalizados. El precio y la superficie deben ser flotantes. La fecha de publicación debe ser un objeto datetime, si existe. Si no existe informacion, o no estas seguro, no incluyas el campo en el JSON."

LAND_PROMPT = "You are helpful assistant, you take a land description and return a JSON with parsed data about the land. The data should include the following fields: price, currency, plot_area, price_per_square_meter, publication_date and easybroker_id. The data should be cleaned and normalized. The price, plot_area and price_per_square_meter should be floats. The publication_date should be a datetime object. The easybroker_id is a string. If there is no information, or you are not sure, do not include the field in the JSON."

def parse_description(description, property_kind, publication_date, crawling_date, model="gpt-4o"):
    response = client.chat.completions.create(
    model=model,
    response_format={ "type": "json_object" },
    temperature= 0,
    messages=[
        {"role": "system", "content": HOUSE_PROMPT if property_kind == "casa" else LAND_PROMPT},
        {"role": "user", "content": f"Publicado {publication_date}, la fecha de hoy es {crawling_date}. \n {description}"}
    ]
    )
    try:
        response_json = json.loads(response.choices[0].message.content)
    except JSONDecodeError:
        response_json = {}
    return response_json

if not exists(input_file):
    print("File not found")
    exit()

df = pd.read_json(input_file)
df.drop_duplicates(subset="url", inplace=True)
df["clean_price"], df["currency"] = zip(*df["price"].map(clean_price))
df["clean_plot_area"] = df["plot_area"].map(clean_area)

df["clean_plot_area"] = df["clean_plot_area"].combine_first(df["build_area"])
df["price_per_m2"] = df["clean_price"] / df["clean_plot_area"]

if os.path.exists(data_pickle):
    existing_props = pd.read_pickle(data_pickle)
    existing_props.reset_index(drop=True, inplace=True)
    # existing_props = existing_props[~existing_props["url"].isin(df["url"])]
else:
    existing_props = pd.DataFrame(columns=df.columns)

new_props = df[~df["url"].isin(existing_props["url"])]
if not new_props.empty:
    new_props.loc[new_props.clean_price < 20000, "price_per_m2"] = new_props.loc[new_props.clean_price < 20000, "clean_price"]
    new_props.loc[new_props.clean_price < 20000, "clean_price"] = None
    new_props.reset_index(drop=True, inplace=True)



    # new_props["in_db"] = df["url"].map(lambda x: exists(f"images/{x.split('/')[-1]}.jpg"))
    # df.loc["clean_price"] = df["clean_price"].combine_first(df["price_per_m2"] * df["clean_plot_area"])


    parsed_data = []

    for i, row in new_props.iloc[len(parsed_data):].iterrows():
        print(i, len(new_props))
        data = parse_description(row["description"], row["property_kind"], row["publication_date"], crawling_date)
        data["url"] = row["url"]
        parsed_data.append(data)

    parsed_df = pd.DataFrame(parsed_data)
    parsed_df.loc[parsed_df.price_per_square_meter == 0, "price_per_square_meter"] = None

    parsed_df.rename(columns={"price": "llm_price", "plot_area": "llm_plot_area", "price_per_square_meter": "llm_price_per_m2", "currency": "llm_currency",
                            "rooms": "llm_rooms", "bathrooms": "llm_bathrooms",
                            "publication_date": "clean_date", "build_area": "llm_build_area", "amenities": "llm_amenities"}, inplace=True)
    extra_columns = set(parsed_df.columns) - set(new_props.columns)

    parsed_df = parsed_df[list(extra_columns) + ['url']]

    new_props = new_props.merge(parsed_df, on="url")
    new_props.reset_index(drop=True, inplace=True)

    result_df = pd.concat([existing_props, new_props])

    result_df.to_pickle(data_pickle)
else:
    print("No new properties")

terrenos_yucatan = result_df[(result_df.state == "yucatan") & (result_df.property_kind == "terreno")]

terrenos_yucatan["price_per_m2"] = terrenos_yucatan["price_per_m2"].combine_first(terrenos_yucatan["llm_price_per_m2"])
terrenos_yucatan["clean_plot_area"] = terrenos_yucatan["clean_plot_area"].combine_first(terrenos_yucatan["llm_plot_area"])
terrenos_yucatan["clean_price"] = terrenos_yucatan["clean_price"].combine_first(terrenos_yucatan["price_per_m2"] * terrenos_yucatan["clean_plot_area"])
terrenos_yucatan["clean_price"] = terrenos_yucatan["clean_price"].combine_first(terrenos_yucatan["llm_price"])
terrenos_yucatan["price_per_m2"] = terrenos_yucatan["price_per_m2"].combine_first(terrenos_yucatan["clean_price"] * terrenos_yucatan["clean_plot_area"])


min_plot = 1000
big_land = terrenos_yucatan[(terrenos_yucatan.clean_plot_area > min_plot)&(~terrenos_yucatan.publisher_phone.isin(contacted))]

big_land.sort_values("price_per_m2", ascending=True).iloc[0]

max_price = 600000
cheap_land = big_land[(big_land.clean_price < max_price)]

cheap_land.sort_values("price_per_m2", ascending=True).iloc[0]


munis = terrenos_yucatan[(terrenos_yucatan.municipality.isin(municipalities_yucatan))&(pd.isnull(terrenos_yucatan.price_per_m2))]


for i, row in terrenos_yucatan.iterrows():
    if isinstance(row.clean_plot_area, float):
        continue
    else:
        print(row.name, row.clean_plot_area)
from typing import Collection, List

from pandas.core.frame import DataFrame
from arango import ArangoClient
from invoke import task
import openpyxl
import pandas as pd
import os
import json
import requests

ARANGODB_USER_NAME = 'root'
ARANGODB_PASSWORD = 'password'
ARANGODB_KA_DMO_DB_NM = 'knowledge_admin_dmo'
DATA_DIR = 'import-arango'
COUNT_INSERTED = 0

client = ArangoClient(hosts="http://localhost:8529")
db = client.db(ARANGODB_KA_DMO_DB_NM, username=ARANGODB_USER_NAME, password=ARANGODB_PASSWORD)

STORIED_DISH_CACHE = []
STORIED_DISH_CATAGORY_CACHE = []
MENUITEM_CACHE= []
MENUITEM_CATAGORY_CACHE = []
SERVING_TEMP_CACHE = []
CUISINE_CACHE = []
SPICINESS_CACHE = []
EATERCLASS_CACHE = []
COOKING_CACHE = []

COLUMN_COLLECTION_MAPPING = {'menuitem': 'FnbMenuItem', 'storied_dish': 'FnbStoriedDish', 'storied_dish_category':'FnbStoriedDishCategory', 
'menuitemcatagory': 'FnbMenuItemCategory', 'servingtemperature':'FnbServingTemperature', 'cuisine':'FnbCuisine', 'spiciness':'FnbSpiciness', 
'eaterclass':'FnbDietaryRestriction', 'ingredient':'FnbIngredient', 'certification':'FnbReligiousDietaryCertification', 
'local_currency':'Currency', 
}

COLLECTION_DATA = {}

DEFAULT_LANG_CODE = 'zh-tw'

def truncate_collection(collection_name):
    collection = db.collection(collection_name)
    collection.truncate()

def get_all(collection):
    query = f"""
    FOR d in {collection}
        return d
    """
    cursor = db.aql.execute(query)
    docs = [doc for doc in cursor]
    return docs

def get_place_id(place_name: str):
    query = """
    FOR d in  Place
        FILTER d.name.default_value == @default_value
    return d
    """
    cursor = db.aql.execute(query, bind_vars={'default_value': place_name})
    docs = [doc for doc in cursor]
    return docs

def build_cache():
    for key,value in COLUMN_COLLECTION_MAPPING.items():
        data = get_all(value)
        name_id_dict = {}
        for item in data:
            for lang, name in item['name']['translations'].items():
                name_id_dict[name] = item['_id']
            name_id_dict[item['name']['default_value']] = item['_id']
        COLLECTION_DATA[key] = name_id_dict

def get_places_by_name(collection_name: str, default_value: str) -> str:
    query = f"""
    FOR d in {collection_name}
        FILTER d.name.default_value == @default_value
    return d
    """
    cursor = db.aql.execute(query, bind_vars={'default_value': default_value})
    docs = [doc for doc in cursor]
    return docs

def get_business_id(place_id: str) -> str:
    query = f"""
    FOR d in Business
        FILTER d.place == @place_id
    return d
    """
    cursor = db.aql.execute(query, bind_vars={'place_id': place_id})
    docs = [doc for doc in cursor]
    return docs[0]['_id'] if docs else None

@task
def import_fnb_dish_data(ctx):
    truncate_collection('FnbItem')
    all_ws_dict = pd.read_excel(os.path.join(DATA_DIR, 'iMarts 6月展示用.xlsx'), sheet_name=None, engine='openpyxl')
   
    df = all_ws_dict['List of Dish ID']
    build_cache()
    import_dish(df)

def get_id_list_for_array(key, items):
    result = []
    for item in items:
        value = get_attr_id(key, item.strip())
        if value:
            result.append(value)
    return result if len(result) > 0 else None

def get_full_object(info_name, row):
    value_zh_tw = row[f'{info_name}_zh-tw']
    value_en = row[f'{info_name}_en']
    translations = {}

    if value_zh_tw and not pd.isnull(value_zh_tw):
        translations['zh-tw'] = value_zh_tw
        if value_en and not pd.isnull(value_en):
            translations['en'] = value_en
        result = {
            'default_value': value_zh_tw,
            'default_lang_code': 'zh-tw',
            'translations': translations
        }
        return result
    return None


def insert_dish_doc(req_body):
    url = "http://localhost:8000/api/dmo/fnb-items"
    try:
        headers_dict = {"x-kami-login-group": "root"}
        res = requests.post(url, data=req_body,
        headers=headers_dict
        )
        if res.status_code == 200:
            global COUNT_INSERTED
            COUNT_INSERTED += 1
            print('Insert ' + str(COUNT_INSERTED) + ' successfully')
        else:
            print (res.json())
    except requests.exceptions.HTTPError as e:
        print (e.response)
        print('Error found ')
        exit(1)

def get_attr_id(key, col_value):
    if col_value and not pd.isnull(col_value):
        if COLLECTION_DATA[key] and col_value in COLLECTION_DATA[key]:
            id = COLLECTION_DATA[key][col_value.strip()]
            return id
    return None

def get_price_obj(price_str):
    try:
        price = int(price_str)
        price_obj = {
            "currency_code": "NTD",
            "value": price
        }
        return price_obj
    except ValueError:
        return None;

def import_dish(df: DataFrame):
    for index, row in df.iterrows():
        if row['store_name_zh'] and not pd.isnull(row['store_name_zh']):
            # key = row["kami_dish_id"].split('/')[1]
            places = get_places_by_name('Place', row['store_name_zh'].strip())

            place_id = places[0]['_id'] if len(places) > 0 else None
            # for place in places:
            #     if 'dmo-' in place['_key']:
            #         place_id = place['_id']
            #         break

            print(row['store_name_zh'].strip())
            if not place_id:
                print(places)
                continue
            print(place_id)
            business_id = get_business_id(place_id)
            print(business_id)
            name_obj = get_full_object('name', row)
            recipe_obj = get_full_object('receipe', row)
            description_obj = get_full_object('description', row)

            dietary_restrictions = row['eaterclass.name'].split(',') if row['eaterclass.name'] and not pd.isnull(row['eaterclass.name']) else []
            # religious_dietary_certifications = row['certification'].split(',') if row['certification'] else []
            religious_dietary_certifications =[]
            ingredients =[]
            # ingredients = row['ingredient.name(s)'].split(',') if row['ingredient.name(s)'] else []

            alt_name_arr = []
            if row['alt_name_zh-tw'] and not pd.isnull(row['alt_name_zh-tw']):
                alt_name_arr.append(row['alt_name_zh-tw'])
            if row['alt_name_en'] and not pd.isnull(row['alt_name_en']):
                alt_name_arr.append(row['alt_name_en'])

            direct_order_url = row['direct_order_url'] if row['direct_order_url'] and not pd.isnull(row['direct_order_url']) else None
            price_obj = get_price_obj(row['local_price']) if row['local_price'] and not pd.isnull(row['local_price']) else None
            dietary_restriction_ids = get_id_list_for_array('eaterclass', dietary_restrictions)
            religious_dietary_certification_ids = get_id_list_for_array('certification', religious_dietary_certifications)
            ingredient_ids = get_id_list_for_array('ingredient', ingredients)

            dish_doc = {
                "business": business_id,
                "name":name_obj,
                "alt_names": alt_name_arr,
                "direct_order_url": direct_order_url,
                "price": price_obj,
                "storied_dish": get_attr_id("storied_dish", row["storied_dish.name"]) ,
                "storied_dish_category": get_attr_id("storied_dish_category", row["storied_dish_category.name"]),
                "recipe": recipe_obj, 
                "description": description_obj, 
                "menu_item": get_attr_id("menuitem", row["menuitem.name"]),
                "menu_item_category": get_attr_id("menuitemcategory", row["menuitemcategory.name"]),
                "knowledge":{
                    "serving_temperature": get_attr_id("servingtemperature", row["servingtemperature.name"]),
                    "cuisine": get_attr_id("cuisine", row["cuisine.name"]),
                    "spiciness": get_attr_id("spiciness", row["spiciness.name"]),
                    "dietary_restrictions": dietary_restriction_ids,
                    "religious_dietary_certifications": religious_dietary_certification_ids
                },
                "ingredients": ingredient_ids
            }
            json_obj = json.dumps(dish_doc, indent = 4)
            insert_dish_doc(json_obj)


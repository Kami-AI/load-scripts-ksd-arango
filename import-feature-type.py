from typing import Collection, List

from pandas.core.frame import DataFrame
from arango import ArangoClient
from invoke import task
import openpyxl
import pandas as pd
import os
import json
import asyncio
import re

ARANGODB_USER_NAME = 'root'
ARANGODB_PASSWORD = 'password'
ARANGODB_KA_DMO_DB_NM = 'knowledge_admin_dmo'
DATA_DIR = 'import-arango'

client = ArangoClient(hosts="http://localhost:8529")
db = client.db(ARANGODB_KA_DMO_DB_NM, username=ARANGODB_USER_NAME, password=ARANGODB_PASSWORD)
DEFAULT_LANG_CODE = 'zh-tw'

def export_json(name: str, data: dict) -> str:
    export_path = os.path.join(DATA_DIR, 'output', name + '.json')
    with open(export_path, 'w+') as outfile:
        json.dump(data, outfile, 
                ensure_ascii=False)
    return export_path

@task
def import_interface_mapping(ctx):
    all_ws_dict = pd.read_excel(os.path.join(DATA_DIR, 
    'iMarts Interface Mapping v1.25.xlsx'), sheet_name=None, engine='openpyxl')
   
    # industry_collection_name = 'Industry'
    # if db.has_collection(industry_collection_name):
    #     db.delete_collection(industry_collection_name)
    # db.create_collection(industry_collection_name)

    place_feature_df = all_ws_dict['industry_can_pick_feature']
    feature = 'Feature'
    feature_group = 'FeatureGroup'
    do_import(feature, feature_group, True, None, place_feature_df, False)

    place_type_df = all_ws_dict['industry_can_pick_type_of_place']
    type  = 'Type'
    type_category = 'TypeCategory'
    do_import(type, type_category, True, None, place_type_df, False)

    activity_feature_df = all_ws_dict['activity_feature_group_can_pick']
    domain = 'Activity'
    do_import(feature, feature_group, False, domain, activity_feature_df, True)

    product_feature_df = all_ws_dict['product_feature_group_can_pick_']
    domain = 'RtlItem'
    do_import(feature, feature_group, False, domain, product_feature_df, True)

    activity_type_df = all_ws_dict['activity_category_can_pick_type']
    domain = 'Activity'
    do_import(type, type_category, False, domain, activity_type_df, True)

    product_type_df = all_ws_dict['product_category_can_pick_type_']
    domain = 'RtlItem'
    do_import(type, type_category, False, domain, product_type_df, True)

def do_import(attribute_name: str, attribute_group_name: str, hasIndustry: bool, 
                domain:str, df: DataFrame, upsert = True):
    attribute_collection_name = attribute_name

    if not upsert:
        if db.has_collection(attribute_collection_name):
            db.delete_collection(attribute_collection_name)
        db.create_collection(attribute_collection_name)

    attribute_group_collection_name = attribute_group_name
    if not upsert:
        if db.has_collection(attribute_group_collection_name):
            db.delete_collection(attribute_group_collection_name)
        db.create_collection(attribute_group_collection_name)

    attribute_selection_collection_name = attribute_name + 'Selection'
    if not upsert:
        if db.has_collection(attribute_selection_collection_name):
            db.delete_collection(attribute_selection_collection_name)
        db.create_collection(attribute_selection_collection_name)
    attribute_selection_list = gen_attribute_selection_list(attribute_collection_name, attribute_group_collection_name, hasIndustry, domain, df)
    load_records_to_arrangodb(attribute_selection_list, attribute_selection_collection_name)


def gen_attribute_selection_list(attribute_name: str, attribute_group_name: str, 
hasIndustry: bool, domain:str, df: DataFrame):
    attribute_selections = []
    last_industry = ''
    last_attribute_group = ''
    industry_id = ''
    attribute_group_id = ''
    attribute_id = ''
    attribute_group_collection = attribute_group_name
    attr_name_in_selection = get_tokenized_name_with_under_score(attribute_name)
    attribute_group_name_in_selection = get_tokenized_name_with_under_score(attribute_group_name)
    
    for row in df.itertuples():
        industry = None
        if hasIndustry:
            industry = row[1]
            attribute_group = row[2]
            attribute = row[3]
        else:
            attribute_group = row[1]
            attribute = row[2]
            
        if (not pd.isnull(industry) or domain) and not pd.isnull(attribute_group) and not pd.isnull(attribute):
            # print(industry)
            
            if hasIndustry:
                industry_id = get_id_by_name('Industry', industry)
                if not industry_id:
                    industry_id = add_new_record('Industry', industry)['_id']
            if last_attribute_group != attribute_group:
                attribute_group_id = add_new_record(attribute_group_collection, attribute_group)['_id']

            attribute_id = add_new_record(attribute_name, attribute)['_id']
            selection = {
                    '{col_name}'.format(col_name=attribute_group_name_in_selection.lower()): attribute_group_id,
                    '{col_name}'.format(col_name=attr_name_in_selection.lower()): attribute_id
            }
            if hasIndustry:
                selection['industry'] = industry_id
                selection['domain'] = 'Place'
            if domain:
                selection['domain'] = domain
            attribute_selections.append(selection)
            last_attribute_group = attribute_group
                
    return attribute_selections

def get_tokenized_name_with_under_score(name):
    name_tokens = re.findall('[A-Z][^A-Z]*',name)
    if len(name_tokens) >= 2:
        name_vs_underscore = ''
        for i in range(len(name_tokens)):
            name_vs_underscore += name_tokens[i]
            if i < len(name_tokens) - 1:
                name_vs_underscore += '_'
    else:
        name_vs_underscore = name
    return name_vs_underscore

def get_id_by_name(collection_name: str, default_value: str) -> str:
    query = """
    FOR d in {collectionName}
        FILTER d.name.default_value == @default_value
    return d
    """.format(collectionName=collection_name)
    cursor = db.aql.execute(query, bind_vars={'default_value': default_value})
    docs = [doc for doc in cursor]
    # if collection_name == 'PlaceType':
    #     print(docs)
    return docs[0]['_id'] if docs else None

def add_new_record(collection_name, value):
    collection = db[collection_name]
    record = construct_insert_dict(value)
    print('Adding '+ value + ' in '+ collection_name)
    print(record)
    return collection.insert(record)

def construct_insert_dict(default_value):
    translations = {}
    default_lang_code = 'zh-tw'
    translations[default_lang_code] = default_value

    record = {
        'name': {
            'default_lang_code': default_lang_code,
            'default_value': default_value,
            'translations': translations,
        }
    }
    return record

def load_records_to_arrangodb(records, collection_name):
    export_json_path = export_json(collection_name, records)  
    collection = db[collection_name]
    print('Loading '+ collection_name + '***********************')
    with open(export_json_path, 'r') as file:
        data = json.load(file)
        collection.import_bulk(data) 

import sys
import json
import csv
import yaml

import pandas as pd
import numpy as np

import matplotlib as mpl

import time
from datetime import datetime 

import pprint

import psycopg2
from sqlalchemy import create_engine, text as sql_text


# ==============================


# Simple check
def hello_world():
    print("Hello World!")

def build_query_reviews_count(date1, date2):
    q21 = """
    SELECT count(*)
    FROM reviews
    WHERE date >= '"""
    q22 = """'
    AND date <= '"""
    q23 = """';
    """
    return q21 + date1 + q22 + date2 + q23

def build_query_listings_join_reviews(date1, date2):
    q24 = """
    SELECT DISTINCT l.id, l.name
    FROM listings l, reviews r
    WHERE l.id = r.listing_id
      AND r.datetime >= '"""
    q25 = """'
      AND r.datetime <= '"""
    q26 = """'
    ORDER BY l.id;
    """
    return q24 + date1 + q25 + date2 + q26

def build_query_text_search_with_index(date1,date2,word):
    q27 = """
    SELECT COUNT(*)
    FROM reviews r
    WHERE comments_tsv @@ to_tsquery('"""
    q28 = """')
    AND datetime >= '"""
    q29 = """'
    AND datetime <= '"""
    return q27 + word + q28 + date1 + q29 + date2 + "';"

def build_query_text_search_without_index(date1,date2,word):
    q30 = """
    SELECT COUNT(*)
    FROM reviews r
    WHERE comments ILIKE('%%"""
    q31 = """%%')
    AND datetime >= '"""
    q32 = """'
    AND datetime <= '"""
    return q30 + word + q31 + date1 + q32 + date2 + "';"

def build_query_update_datetimes_neigh_group_add(neigh_group):
    q33 = """
    UPDATE reviews r 
    SET datetime = datetime + interval '5 days' 
    FROM listings l 
    WHERE l.id = r.listing_id 
    AND l.neighbourhood_group = '"""
    q34 = """'
    RETURNING 'done';"""
    return q33 + neigh_group + q34

def build_query_update_datetimes_neigh_group_minus(neigh_group):
    q33 = """
    UPDATE reviews r 
    SET datetime = datetime - interval '5 days' 
    FROM listings l 
    WHERE l.id = r.listing_id 
    AND l.neighbourhood_group = '"""
    q34 = """'
    RETURNING 'done';"""
    return q33 + neigh_group + q34

def build_query_update_datetimes_neigh_add(neigh):
    q33 = """
    UPDATE reviews r 
    SET datetime = datetime + interval '5 days' 
    FROM listings l 
    WHERE l.id = r.listing_id 
    AND l.neighbourhood = '"""
    q34 = """'
    RETURNING 'done';"""
    return q33 + neigh + q34

def build_query_update_datetimes_neigh_minus(neigh):
    q33 = """
    UPDATE reviews r 
    SET datetime = datetime - interval '5 days' 
    FROM listings l 
    WHERE l.id = r.listing_id 
    AND l.neighbourhood = '"""
    q34 = """'
    RETURNING 'done';"""
    return q33 + neigh + q34

def time_diff(time1, time2):
    return (time2-time1).total_seconds()

def get_run_time_stats_single_query(db_eng, count, query):
    time_list = []        
    for i in range(0,count): 
        time_start = datetime.now()

        with db_eng.connect() as conn:
            df = pd.read_sql(query, con=conn)

        time_end = datetime.now()
        diff = time_diff(time_start, time_end)
        time_list.append(diff)
            
    # pprint.pp(time_list)
    # print(round(sum(time_list)/len(time_list), 4), \
    #     round(min(time_list), 4), \
    #     round(max(time_list), 4), \
    #     round(np.std(time_list), 4))

    perf_profile = {}
    perf_profile['avg'] = round(sum(time_list)/len(time_list), 4)
    perf_profile['min'] = round(min(time_list), 4)
    perf_profile['max'] = round(max(time_list), 4)
    perf_profile['std'] = round(np.std(time_list), 4)
    perf_profile['count'] = count
    perf_profile['timestamp'] = time_start.strftime('%Y-%m-%d-%H:%M:%S')
    return perf_profile

# last 3 params should be strings
def add_drop_index(db_eng, add_or_drop, col, table):
    q_create_index = f'''
    BEGIN TRANSACTION;
    CREATE INDEX IF NOT EXISTS {col}_in_{table}
    ON {table}({col});
    END TRANSACTION;
    '''

    q_drop_index = f'''
    BEGIN TRANSACTION;
    DROP INDEX IF EXISTS {col}_in_{table};
    END TRANSACTION;
    '''

    q_show_indexes = f'''
    select *
    from pg_indexes
    where tablename = '{table}';
    '''

    with db_eng.connect() as conn:
        if add_or_drop == 'add':
            conn.execute(sql_text(q_create_index))
        elif add_or_drop == 'drop':
            conn.execute(sql_text(q_drop_index))
        result = conn.execute(sql_text(q_show_indexes))
        return result.all()

# fetches filename (which should be a json file) and returns a 
#       dict corresponding to the contents of filename
def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)

# writes the dictionary in dict as a json file into filename
def write_perf_data(dict, filename):
    with open('perf_data/' + filename, 'w') as fp:
        json.dump(dict, fp)

def build_index_description_key(all_indexes, spec):
    key_value = "__"
    for index in all_indexes:
        if index in spec:
            key_value = key_value + f"{index[0]}_in_{index[1]}__"
    return key_value

def full_value_summary(db_eng, query, query_name, spec, all_indexes, count, json_file_name):
    # perf_summary = fetch_perf_data('perf_summary.json')
    perf_summary = fetch_perf_data(json_file_name)

    # drop unused indexes
    for index in all_indexes:
        if index not in spec:
            # print(index[0],index[1])
            add_drop_index(db_eng, 'drop', index[0], index[1])
            
    # add indexes to the corresponding tables
    for index in spec:
        add_drop_index(db_eng, 'add', index[0], index[1])

    # get run time stats
    perf_profile = get_run_time_stats_single_query(db_eng, count, query)

    # create these description_keys
    key_value = build_index_description_key(all_indexes, spec)

    # before modification: get previous data in perf_summary first
    if query_name in perf_summary:
        perf_dict = perf_summary[query_name]
    else:
        perf_dict = {}
    
    # actually complete the modification
    perf_dict[key_value] = perf_profile
    perf_summary[query_name] = perf_dict

    # write_perf_data(perf_summary, 'perf_summary.json')
    write_perf_data(perf_summary, json_file_name)
    return perf_summary

# Function to rename keys and extract the year
def rename_keys(data):
    new_data = {}
    for key, value in data.items():
        if key.startswith("listings_join_reviews_"):
            year = key.split("_")[-1]
            new_data[year] = value
        else:
            new_data[key] = value
    return new_data

def rename_keys_text_search(data, word):
    new_data = {}
    for key, value in data.items():
        if key.startswith(f"{word}_"):
            year = key.split("_")[-1]
            new_data[year] = value
        else:
            new_data[key] = value
    return new_data

def rename_keys_updates(data):
    new_data = {}
    for key, value in data.items():
        if key.startswith("update_datetimes_query_"):
            year = key.split("_")[-1]
            new_data[year] = value
        else:
            new_data[key] = value
    return new_data
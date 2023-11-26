from flask import Flask, request
import os
from supabase import create_client
import requests
import json
import re
import pandas as pd

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase= create_client(url, key)

app = Flask(__name__)

@app.route("/get-canonical")
def get_groceries_data():
    url = "https://heisse-preise.io/data/latest-canonical.json"
    resp = requests.get(url=url)
    data = resp.json()
    return data
    
def save_data(data):
    norm_data = replace_unicode_characters(data)
    for grocerie in norm_data:
        try:
            grow = replace_unicode_characters(grocerie)
            supabase.table("groceries").insert(grow).execute()
        except Exception as e:
            # Handle the exception for each individual insert
            print(f"An error occurred for grocerie {grocerie}: {e}")

def delete_stores(data, stores):
    for store in stores:
        data = data.drop(data[data['Store'] == store].index)
    return data

def filter_stores(data, stores):
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
    # Use the query method to filter rows where the 'Category' column is in the specified categories
    data = data.query(f'Store in {stores}')
    return data

def filter_category(data, categories):
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
    # Use the query method to filter rows where the 'Category' column is in the specified categories
    data = data.query(f'Category in {categories}')
    return data

def replace_unicode_characters(json_data):
    if isinstance(json_data, dict):
        return {key: replace_unicode_characters(value) for key, value in json_data.items()}
    elif isinstance(json_data, list):
        return [replace_unicode_characters(item) for item in json_data]
    elif isinstance(json_data, str):
        # Use regex to remove backslash-escaped words
        cleaned_string = re.sub(r'\\[a-zA-Z0-9]+', '', json_data)
        return cleaned_string
    else:
        return json_data

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/test")
def test():
    print("Test")
    try:
        data, count = supabase.table('groceries').select('*').like('name', '%active%').execute()
    except Exception as e:
            # Handle the exception for each individual insert
            print(f"An error occurred for grocerie {data}: {e}")    
    #Assert we pulled real data.
    json_data = data[1]
    for i in json_data: 
        print(i["name"])
    return data[1][1]

@app.route("/groceries", methods=['GET'])
def get_Groceries():
    if request.method == 'GET':
        searchword = request.form['name']
        try:
            data, count = supabase.table('groceries').select('*').like('name', '%{}%'.format(searchword)).execute()
        except Exception as e:
                # Handle the exception for each individual insert
                print(f"An error occurred for grocerie {data}: {e}")    
        json_data = data[1]
        print(json_data)
        product_names = []
        for i in json_data: 
            print(i["name"])
            product_names.append(i["name"])
    return product_names

@app.route("/cheap", methods=['GET'])
def whats_cheap():
    searchword = "banane"
    try:
       data, count = supabase.table('groceries') \
            .select('name, new_price, priceHistory->0->price') \
            .lt('new_price', float('priceHistory->0->>price')) \
            .execute()
    except Exception as e:
            # Handle the exception for each individual insert
             print(f"An error occurred for groceries {data}: {e}")
    json_data = data[1]
    return json_data

def get_second_price(history):
    if history and isinstance(history, list) and len(history) > 1:
        return history[1]['price']
    else:
        return None

def data_prep():
    data = get_groceries_data()
    df = pd.json_normalize(data)
    # Calculate the average price (assuming 'priceHistory' is always present)
    df['average_price'] = df['priceHistory'].apply(lambda history: sum(entry['price'] for entry in history if entry.get('price') is not None) / len(history) if history else None)
    # Extract the newest old price (first entry in "priceHistory")
    df['old_price'] = df['priceHistory'].apply(get_second_price)    
    # Extract the newest old price (first entry in "priceHistory")
    df['new_price'] = df['price']
    # Create a column for the difference between old and new price
    df['price_difference'] = df['old_price'] - df['new_price']
    # Sort by the absolute value of the price difference
    df = df.reindex(df['price_difference'].abs().sort_values(ascending=False).index)
    # Create a column for the percentage of the difference
    df['percentage_difference'] = (df['price_difference'] / df['new_price']) * 100
    print("DATA PREPARED: ")
    print(df)
    return df

def create_topten(data):
    topten = pd.DataFrame(columns=['Name','Store','OldPrice','NewPrice','PriceDifference','Percentage','AveragePrice', 'Category'])
    topten['Name']=data['name']
    topten['Store']=data['store']
    topten['OldPrice']=data['old_price']
    topten['NewPrice']=data['new_price']
    topten['PriceDifference']=data['price_difference']
    topten['Percentage']=data['percentage_difference']
    topten['AveragePrice']=data['average_price']
    topten['Category']=data['category']
    return topten

@app.route("/topten", methods=['GET'])
def top_ten():
    data = data_prep()
    topten = create_topten(data)

    stores = ['hofer', 'lidl']
    #grundnahrungCat = ['50','51','52', '53', '54', '55', '56', '57', '58', '59', '5A', '5B', '5C']
    categories = ['00','01','02','03',"10","11","12","13","14",'30','31','32', '33', '34', '35', '36', '37', '38', '39', '3A', '3B','50','51','52', '53', '54', '55', '56', '57', '58', '59', '5A', '5B', '5C','5D', '5E', '5F']
    topten = filter_category(topten, categories)
    topten = filter_stores(topten, stores)

    print("TOPTEN: ")
    print(topten.head(10))   

    posTop = topten[topten['PriceDifference'] > 0.0] 
    print("TOPTEN over 0 diff: ")
    print(posTop.head(25))
    return posTop

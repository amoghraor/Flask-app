import json
from elasticsearch import Elasticsearch, helpers
from flask import Flask
app = Flask(__name__)
def connect_to_elastic(cloud_id, api_key):
    """
    Connect to Elasticsearch using the Cloud ID and API Key.
    """
    try:
        es = Elasticsearch(
            cloud_id=cloud_id,
            api_key=api_key
        )
        if es.ping():
            print("Connected to Elasticsearch!")
            return es
        else:
            print("Failed to connect to Elasticsearch.")
            return None
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return None

def create_index_with_mapping(es, index_name):
    """
    Create an Elasticsearch index with a predefined mapping.
    """
    try:
        # Define the mapping
        mapping = {
            "mappings": {
                "properties": {
                    "Name": {"type": "text"},
                    "Miles_per_Gallon": {"type": "float"},
                    "Cylinders": {"type": "integer"},
                    "Displacement": {"type": "float"},
                    "Horsepower": {"type": "float"},
                    "Weight_in_lbs": {"type": "integer"},
                    "Acceleration": {"type": "float"},
                    "Year": {"type": "date", "format": "yyyy-MM-dd"},
                    "Origin": {"type": "keyword"}
                }
            }
        }

        # Create the index
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            print(f"Index '{index_name}' created successfully!")
        else:
            print(f"Index '{index_name}' already exists.")
    except Exception as e:
        print(f"Error creating index: {e}")

def load_data_to_elasticsearch(es, index_name, file_path):
    """
    Load JSON data from a file and index it into Elasticsearch.
    """
    try:
        # Load data from JSON file
        with open(file_path, "r") as f:
            data = json.load(f)

        # Use helpers.bulk for efficient indexing
        actions = [
            {
                "_index": index_name,
                "_source": record
            }
            for record in data
        ]
        helpers.bulk(es, actions)
        print(f"Data from '{file_path}' indexed successfully!")
    except Exception as e:
        print(f"Error loading data to Elasticsearch: {e}")

def search_by_keyword(es, index_name, keyword, mname):
    """
    Search Elasticsearch index for documents using a keyword.
    """
    try:
        query = {
            "query": {
                "match": {
                    keyword: mname
                }
            }
        }

        response = es.search(index=index_name, body=query)
        hits = response.get('hits', {}).get('hits', [])
        
        if not hits:
            print(f"No documents found for '{mname}' in field '{keyword}'.")
            return {"success":"this works!"}
        else:
            print(f"Found {len(hits)} result(s) for '{mname}':")
            for hit in hits:
                source = hit['_source']
                print(json.dumps(source, indent=4))
                return {"success":f"{json.dumps(source, indent=4)}"}
    except Exception as e:
        print(f"Error searching Elasticsearch: {e}")
        return {"failure":e}
@app.route("/")
def main_route():
    """
    Main function to create an index, load data, and search in Elasticsearch.
    """
    # Input Elastic Cloud credentials
    cloud_id = "46a8ed2426cd48cdaefd07c667cfcf05:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxODUyOTlmOWViZGE0MzdhOTc0MTRjOGYwMjMwNTEzZSRmYzE2ZDQyMzIxNGM0YTA5YmFkOWM2N2FlODExNTU1NQ=="
    api_key = "SmVtMmZwTUJldUpZWHBXUktsN2Q6QTNGQkVkZWhUb3VXalUwUXl3UDBHdw=="

    # Connect to Elasticsearch
    es = connect_to_elastic(cloud_id, api_key)
    if not es:
        return

    # Define index name
    index_name = "cars"

    # Create index with mapping
    create_index_with_mapping(es, index_name)

    # Load data from JSON file
    file_path = "cars.json"  # Replace with "cars (1).json" if needed
    load_data_to_elasticsearch(es, index_name, file_path)

    # Search by keyword
    keyword = "Name"  # Elasticsearch field to search
    mname = "buick"
    search_by_keyword(es, index_name, keyword, mname)
    return {"great sucess":f"{search_by_keyword(es, index_name, keyword, mname)}"}

if __name__ == "__main__":
    app.run(debug=True)

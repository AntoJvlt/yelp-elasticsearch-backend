from flask import Flask, abort, jsonify, request
from elasticsearch import Elasticsearch, exceptions
from dotenv import load_dotenv
import os

app = Flask(__name__)

def connect_to_elasticsearch():
    """
        Loads the .env file which should contain
        the elasticsearch connection informations.

        Returns the elasticsearch connection.
    """
    load_dotenv('config.env')
    return Elasticsearch(
        cloud_id=os.getenv('ELASTIC_CLOUD_ID'),
        http_auth=(os.getenv('ELASTIC_USER'), os.getenv('ELASTIC_PASSWORD')),
        timeout=2000
    )

es = connect_to_elasticsearch()
print(es)

@app.route('/search/<query>', methods=['GET'])
def search(query):
    """
        Sends an elasticsearch query which returns 50 results at most.

        The query will try to match the given query string and it will 
        boost the stars field and review_count field.

        If a 'city' parameter is given, the query will filter results
        based on this city.
    """
    if request.args.get('city'):
        query = {
            "function_score": {
                "functions": [
                    {
                    "field_value_factor": {
                        "field": "stars",
                        "factor": 100,
                        "missing": 1
                    }
                    },
                    {
                    "field_value_factor": {
                        "field": "review_count",
                        "factor": 1.2,
                        "modifier": "sqrt",
                        "missing": 1
                    }
                    }
                ],
                "query": {
                    "bool": {
                        "must": {
                            "multi_match" : {
                            "query" : query.lower(),
                            "fields" : [ "name", "city", "state", "categories", "reviews" ]
                            }
                        },
                        "filter": {
                            "term": {
                            "city": request.args.get('city').lower().split(' ')[0] #Get the first keyword of the city
                            }
                        }
                    }
                }
            }
        }
    else:
        query = {
            "function_score": {
                "functions": [
                    {
                    "field_value_factor": {
                        "field": "stars",
                        "factor": 100,
                        "missing": 1
                    }
                    },
                    {
                    "field_value_factor": {
                        "field": "review_count",
                        "factor": 1.2,
                        "modifier": "sqrt",
                        "missing": 1
                    }
                    }
                ],
                "query": {
                    "multi_match" : {
                    "query" : query.lower(),
                    "fields" : [ "name", "city", "state", "categories", "reviews" ] 
                    }
                }
            }
        }

    es_response = es.search(index='business', body={
        "size": 50,
        "query": query,
        "_source": [
            "business_id",
            "name",
            "address",
            "city",
            "state",
            "latitude",
            "longitude",
            "stars",
            "review_count",
            "categories"
        ]
    })

    response = jsonify([doc['_source'] for doc in es_response['hits']['hits']])
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route('/reviews/<business_id>', methods=['GET'])
def get_reviews_by_business_id(business_id):
    """
        Fetchs all the reviews of the given business_id from elasticsearch.

        If the given business_id doesn't exist, 404 HTTP is returned.
    """
    try:
        es_response = es.get(index='business', id=business_id, _source_includes=['reviews'])
    except exceptions.NotFoundError:
        abort(404)

    response = jsonify(es_response['_source']['reviews'])
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

from fastapi import FastAPI, HTTPException
import json
from uuid import uuid4
import pymongo
from bson import ObjectId
from bson.json_util import dumps
from neo4j import GraphDatabase
from pymongo import MongoClient
import requests
import os
import pandas
from neo4j.graph import Node, Relationship

os.environ["base-url"] = "http://localhost:8000"

app = FastAPI()

mongo_client = MongoClient("mongodb://root:example@127.1.1.5:27017/?authMechanism=DEFAULT")
neo4j_client = GraphDatabase.driver("neo4j://neo4j:9001", auth=("neo4j", "password"))


response = requests.post(f"{os.environ['base-url']}/admin/v1/auth", json = {
    "email": "admin@moncon.at",
    "password": "password"
})

auth_token = response.json()['accessToken']





# EMPTY DATABASES

@app.delete("/empty-databases")
async def empty_databases():
    # mongo_list = mongo_client.list_database_names()

    # for database in mongo_list:
    #     if database == 'admin' or database == 'config' or database == 'local':
    #         pass
    #     else:
    #         mongo_client.drop_database(database)

    db = mongo_client["assets"]
    assets_list = db.list_collection_names()

    for collection_assets in assets_list:
        if collection_assets == "asset_type":
            pass
        else:
            db.drop_collection(collection_assets)

    db = mongo_client["devices"]
    devices_list = db.list_collection_names()

    for collection_devices in devices_list:
        if collection_devices == "device_type":
            pass
        else:
            db.drop_collection(collection_devices)


    # with neo4j_client.session() as session:
    #     session.run(
    #         "MATCH (n) DETACH DELETE n"
    #     )

    with neo4j_client.session() as session:
        session.run(
            """MATCH (n) WHERE NOT (n:Service OR (n:Group OR n:User OR n:Role) AND n.uuid IN 
            ["f3aa4c41-bd27-4b9d-91b4-d47b3af0dd8a", "596c21d5-2ed3-432c-8446-48fff941e210",
            "2a7b907d-defc-454f-9e9e-932db9fe0423"]) DETACH DELETE n"""
        )




# CREATE BASIC STRUCTURE
# CREATE CEO GROUP, ROLE AND USER

@app.post("/create-basic-structure")
async def create_basic_structure():
    ceo_response = requests.post(
        f"{os.environ['base-url']}/admin/v1/groups/f3aa4c41-bd27-4b9d-91b4-d47b3af0dd8a",
        json = {
            "name": "Betrieb",
            "delayBetweenUsers": 10000,
            "timeSchedule": True,
            "whiteLabellingEnabled": True 
        },
        headers = {
            "Authorization": f"Bearer {auth_token}"
        }
    )

    ceo_role_response = requests.post(
        f"{os.environ['base-url']}/admin/v1/roles/{ceo_response.json()['uuid']}",
        json = {
            "name": "CEO-Role",
            "permissions": [
                0
            ]
        },
        headers = {
            "Authorization": f"Bearer {auth_token}"
        }
    )

    top_user_response = requests.post(
        f"{os.environ['base-url']}/admin/v1/users/{ceo_response.json()['uuid']}",
        json = {
            "username": "CEO",
            "email": f"jfoawejn{uuid4()}@gmx.at",
            "password": "avfae",
            "phone": "623)60969895",
            "pushId": "string",
            "firebaseToken": "string"
        },
        headers = {
            "Authorization": f"Bearer {auth_token}"
        }
    )

    requests.put(
        f"{os.environ['base-url']}/admin/v1/roles/add-role/{ceo_role_response.json()['uuid']}/{top_user_response.json()['uuid']}",
        headers = {
            "Authorization": f"Bearer {auth_token}"
        }
    )


# CREATE DEPARTMENT GROUP, ROLE AND USER

    departments = 1
    while departments <= 3:
        department_response = requests.post(
            f"{os.environ['base-url']}/admin/v1/groups/{ceo_response.json()['uuid']}",
            json = {
                "name": f"Department {departments}",
                "delayBetweenUsers": 10000,
                "timeSchedule": True,
                "whiteLabellingEnabled": True
            },
            headers = {
                "Authorization": f"Bearer {auth_token}"
            }
        )

        department_role_response = requests.post(
            f"{os.environ['base-url']}/admin/v1/roles/{department_response.json()['uuid']}",
            json = {
                "name": "Leader-Role",
                "permissions": [
                    0
                ]
            }, headers = {
                "Authorization": f"Bearer {auth_token}"
            }
        )

        department_user_response = requests.post(
            f"{os.environ['base-url']}/admin/v1/users/{department_response.json()['uuid']}",
            json = {
                "username": "Leader",
                "email": f"abonewob{uuid4()}@gmx.at",
                "password": "dfaev",
                "phone": "108)7164028",
                "pushId": "string",
                "firebaseToken": "string"
            }, headers = {
                "Authorization": f"Bearer {auth_token}"
            }
        )

        requests.put(
            f"{os.environ['base-url']}/admin/v1/roles/add-role/{department_role_response.json()['uuid']}/{department_user_response.json()['uuid']}",
            headers = {
                "Authorization": f"Bearer {auth_token}"
            }
        )


        # CREATE ASSETS WITH DIFFERENT ID'S

        assets = 1
        asset_types = [
            "64d5b952b107bc72b9ee761d",
            "64d5b952b107bc72b9ee761c"
        ]

        device_types = [
            "64d5b952b107bc72b9ee761b",
            "64d5b952b107bc72b9ee761a"
        ]

        if departments <= 2:
            while assets <= 3:
                if departments == 1:
                    asset_response = requests.post(
                        f"{os.environ['base-url']}/admin/v1/assets/{department_response.json()['uuid']}",
                        json = {
                            "name": f"Control-Unit {assets}",
                            "ui": {},
                            "assetType": f"{asset_types[0]}"
                        }, headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                    asset_channel_response = requests.post(
                        f"{os.environ['base-url']}/admin/v1/asset-channels/{asset_response.json()['id']}",
                        json = {
                            "label": "Hallo",
                            "unit": "Guten Morgen",
                            "timeout": 0,
                            "dataFormat": 0,
                            "digitCount": 0,
                            "scale": 0,
                            "retention": 0,
                            "aggregation": 0
                        }, headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                    device_response = requests.post(
                        f"{os.environ['base-url']}/admin/v1/devices/{department_response.json()['uuid']}",
                        json = {
                            "name": "Furnace",
                            "deviceType": f"{device_types[0]}"
                        }, headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                    requests.put(
                        f"{os.environ['base-url']}/admin/v1/assets/link-device-asset/{asset_response.json()['id']}/{device_response.json()['id']}",
                        headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                else:
                    asset_response = requests.post(
                        f"{os.environ['base-url']}/admin/v1/assets/{department_response.json()['uuid']}",
                        json = {
                            "name": f"Control-Unit {assets}",
                            "ui": {},
                            "assetType": f"{asset_types[1]}"
                        }, headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                    device_response = requests.post(
                        f"{os.environ['base-url']}/admin/v1/devices/{department_response.json()['uuid']}",
                        json = {
                            "name": "Furnace",
                            "deviceType": f"{device_types[1]}"
                        }, headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                    requests.put(
                        f"{os.environ['base-url']}/admin/v1/assets/link-device-asset/{asset_response.json()['id']}/{device_response.json()['id']}",
                        headers = {
                            "Authorization": f"Bearer {auth_token}"
                        }
                    )

                assets += 1
        else:
            pass

        departments += 1


# DROP MONGO TEMPLATE DATABASE

@app.delete("/delete-template-collection")
async def delete_template_collection():
    mongo_client.drop_database("neo4j_template")


# GET NODES AND RELATIONSHIPS FROM NEO4J AND SAVE THEM TO MONGO

@app.get("/save-template/{start_node}/{end_node}")
async def save_template(start_node, end_node):

    with neo4j_client.session() as session:
        result = session.run(
            """MATCH p=shortestPath((g)-[r*]->(a)) WHERE
            (g.uuid = $start_node_uuid OR g.id = $start_node_uuid) AND
            (a.uuid = $end_node_uuid OR a.id = $end_node_uuid) RETURN p""",
            {"start_node_uuid": str(start_node), "end_node_uuid": str(end_node)}
        )

        nodes = []
        relationships = []

        for results in result:
            for x in results:
                for node in x.nodes:
                    nodes.append({
                        "element_id": node.element_id,
                        "labels": list(node.labels),
                        "properties": dict(node._properties)
                    })

                for relationship in x.relationships:
                    relationships.append({
                        "elements_id": relationship.element_id,
                        "start_node_element_id": relationship.start_node.element_id,
                        "end_node_element_id": relationship.end_node.element_id,
                        "type": relationship.type,
                        "properties": dict(relationship._properties)
                    })
                    
        db = mongo_client["neo4j_template"]
        node = db["nodes"]
        relationship = db["relationships"]

        i = len(nodes)
        while i != 0:
            node.insert_one(nodes[-i])
            i -= 1

        i = len(relationships)
        while i != 0:
            relationship.insert_one(relationships[-i])
            i -= 1


# CREATE NEO4J STRUCTURE FROM TEMPLATE
# CREATE NODES FROM MONGO

@app.post("/use-template")
async def use_template():
    db = mongo_client["neo4j_template"]
    nodes_collection = db["nodes"]

    result = nodes_collection.find()

    for results in result:
        with neo4j_client.session() as session:

            node_labels = ""

            if len(results["labels"]) > 0:
                node_labels = f":{':'.join(results['labels'])}"

            properties = results["properties"]
            properties["uuid"] = str(uuid4())
            properties["id"] = str(uuid4())

            session.run(
                f"""
                CREATE (g{node_labels} $properties)
                """,{
                    "properties": properties
                }
            )

@app.get("/test")
async def get_element_id():

    db = mongo_client["neo4j_template"]
    nodes_collection = db["nodes"]
    relationships_collection = db["relationships"]

    nod = nodes_collection.find()
    rel = relationships_collection.find()

    nodes = []
    relationships = []

    for node in nod:
        nodes.append(node)

    for relationship in rel:
        relationships.append(relationship)

    print(nodes[0])


        





# @app.get("/test")
# async def from_mongo_to_neo4j():
#     db = mongo_client["neo4j_template"]
#     nodes_collection = db["nodes"]
    
#     template = nodes_collection.find_one({"_id": ObjectId("64c78135bed0b3705e04f476")})

#     if template is None:
#         return HTTPException(404, "Template not found")
    
#     with neo4j_client.session() as session:
#         node_labels = ""

#         if len(template["labels"]) > 0:
#             node_labels = f":{':'.join(template['labels'])}"

#         properties = template["properties"]
#         properties["uuid"] = str(uuid4())

#         records = session.run(
#             f"""
#             CREATE (g{node_labels} $properties)
#             RETURN g
#             """,
#             {
#                 "properties": properties
#             }
#         )

#         results = records.single()
#         return results

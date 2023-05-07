
import pandas as pd
import pymongo
from pyarrow import fs

fs = fs.HadoopFileSystem("hdfs://127.0.0.1:9000?user=hduser")
file = pd.read_csv(fs.open_input_file("GooglePlaystore.csv"))
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.csv_posts
db = client["data"]
collection_users = db["users"]
collection = db["dataset"]
# add CCT UERT TO USER COLLECTION
collection_users.insert_one({
    "username": "CCT",
    "password": "$2a$10$VWslDXEym0NBktnrHzLih.bZsBzv67HcR2n//VcmKnpkeCj9MXdYe",
    "roles": [{'_class': 'org.springframework.security.core.authority.SimpleGrantedAuthority',
               'role': 'USER'},
              {'_class': 'org.springframework.security.core.authority.SimpleGrantedAuthority',
               'role': 'ADMIN'}]

})
# take file from hadoop and write to mongodb
collection_users.insert_one({
    "username": "CCT",
    "password": "$2a$10$VWslDXEym0NBktnrHzLih.bZsBzv67HcR2n//VcmKnpkeCj9MXdYe",
    "roles": [{'_class': 'org.springframework.security.core.authority.SimpleGrantedAuthority',
               'role': 'USER'},
              {'_class': 'org.springframework.security.core.authority.SimpleGrantedAuthority',
               'role': 'ADMIN'}]

})
# take file from hadoop and write to mongodb

with open(file, encoding="utf8") as file:
    csvReader = csv.reader(file)
    for row in csvReader:
        document = {
            "appName": row[0],
            "appId": row[1],
            "category": row[2],
            "rating": row[3],
            "free": row[8],
            "price": row[9],
            "size": row[11],
            "androidVersion": row[12],
            "developerId": row[13]
        }
        collection.insert_one(document)

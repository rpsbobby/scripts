import pymongo
import csv


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["data"]
collection = db["dataset"]
documents = []
with open("./Google-Playstore.csv", encoding="utf8") as file:
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
        documents.append(document)
        if len(documents) > 300:
            break

print(len(documents))
print(client)
print(collection)

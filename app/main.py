from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import json
from datetime import datetime
import uuid
from datetime import datetime
import uuid

from utils import *
from schemas import *

app = FastAPI()

# CRUD API Endpoints
## Register Broker
@app.post("/register_broker/")
async def register_broker(broker: BrokerRecord):
    filePath = "./metadata.json"
    data = load_data(filePath)
    foundDict = checkBrokerExists(broker.dict(),data["RegisterBrokerRecords"]["records"])
    if(foundDict):
        return foundDict['internal_uuid']
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["RegisterBrokerRecords"]['timestamp'] = timestamp
    serverSetup = broker.dict()
    serverSetup["internal_uuid"] = str(uuid.uuid4())
    serverSetup["brokerStatus"] = "ALIVE"
    serverSetup["epoch"] = 0
    data["RegisterBrokerRecords"]["records"].append(serverSetup)
    save_data(filePath,data)
    return serverSetup["internal_uuid"]


## Get all brokers
@app.get("/get_broker/")
async def get_allbrokers():
    filePath = "./metadata.json"
    data = load_data(filePath)
    return data["RegisterBrokerRecords"]["records"]


## Get broker by id
@app.get("/get_broker/{broker_id}")
async def get_broker_by_ID(broker_id:int):
    filePath = "./metadata.json"
    data = load_data(filePath)
    found_dict = next((broker for broker in data["RegisterBrokerRecords"]["records"] if broker.get("brokerId") == broker_id), None)
    if found_dict:
        return found_dict
    return "Broker Not Found"

## Delete broker by id
@app.delete("/delete_broker/{broker_id}")
async def delete_broker(broker_id: int):
    filePath = "./metadata.json"
    data = load_data(filePath)
    brokers = data["RegisterBrokerRecords"]["records"]
    index = next((index for index, broker in enumerate(brokers) if broker.get("brokerId") == broker_id), None)
    deletedBroker = None
    if index is not None:
        deletedBroker = brokers.pop(index)
    data["RegisterBrokerRecords"]["records"] = brokers
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["RegisterBrokerRecords"]["timestamp"] = timestamp
    save_data(filePath,data)
    return deletedBroker if deletedBroker is not None else "Broker Not Found"

## Register Topic
@app.post("/register_topic/")
async def register_topic(topicRecord:TopicRecord):
    filePath = "./metadata.json"
    data = load_data(filePath)
    found_dict = next((topic for topic in data["TopicRecord"]["records"] if topic.get("name") == topicRecord.name), None)
    if(found_dict):
        return found_dict['topicUUID']
    serverSetup = {"name":topicRecord.name,"topicUUID":str(uuid.uuid4())}
    data["TopicRecord"]["records"].append(serverSetup)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["TopicRecord"]["timestamp"] = timestamp
    save_data(filePath,data)
    return serverSetup["topicUUID"]

## Get Topic by topic name
@app.get("/get_topic/{topicName}")
async def getTopicByName(topicName:str):
    filePath = "./metadata.json"
    data = load_data(filePath)
    found_dict = next((topic for topic in data["TopicRecord"]["records"] if topic.get("name") == topicName), None)
    if(found_dict):
        return found_dict
    return "Topic Not Found"

## Get all topics 
@app.get("/get_topic/")
async def getAllTopics():
    filePath = "./metadata.json"
    data = load_data(filePath)
    return data["TopicRecord"]["records"]

## Delete topic by topic name
@app.delete("/delete_topic/{topicName}")
async def delete_topicByName(topicName: str):
    filePath = "./metadata.json"
    data = load_data(filePath)
    topics = data["TopicRecord"]["records"]
    index = next((index for index, topic in enumerate(topics) if topic.get("name") == topicName), None)
    deletedTopic = None
    if index is not None:
        deletedTopic = topics.pop(index)
    data["TopicRecord"]["records"] = topics
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["TopicRecord"]["timestamp"] = timestamp
    save_data(filePath,data)
    return deletedTopic if deletedTopic is not None else "Topic Not Found"

## Register a partition
@app.post("/register_partition/")
async def register_partition(partitionRecord:PartitionRecord):
    filePath = "./metadata.json"
    data = load_data(filePath)
    found_dict = next((partition for partition in data["PartitionRecord"]["records"] if partition.get("partitionId") == partitionRecord.partitionId), None)
    if(found_dict):
        return found_dict['partitionId']
    serverSetup = partitionRecord.dict()
    data["PartitionRecord"]["records"].append(serverSetup)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["PartitionRecord"]["timestamp"] = timestamp
    save_data(filePath,data)
    return serverSetup["topicUUID"]

## Get partition by prtitionId
@app.get("/get_partition/{partitionId}")
async def get_partitionByID(partitionId:int):
    filePath = "./metadata.json"
    data = load_data(filePath)
    found_dict = next((partition for partition in data["PartitionRecord"]["records"] if partition.get("partitionId") == partitionId), None)
    if(found_dict):
        return found_dict
    return "Partition Not Found"

## Get all partitions
@app.get("/get_partition/")
async def get_allpartitions():
    filePath = "./metadata.json"
    data = load_data(filePath)
    return data["PartitionRecord"]["records"]

## Delete a partition by partitionId
@app.delete("/delete_partition/{partition_id}")
async def delete_partition(partition_id: int):
    filePath = "./metadata.json"
    data = load_data(filePath)
    partitions = data["PartitionRecord"]["records"]
    index = next((index for index, partition in enumerate(partitions) if partition.get("partitionId") == partition_id), None)
    deletedPartition = None
    if index is not None:
        deletedPartition = partitions.pop(index)
    data["PartitionRecord"]["records"] = partitions
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data["PartitionRecord"]["timestamp"] = timestamp
    save_data(filePath,data)
    return deletedPartition if deletedPartition is not None else "Partition Not Found"

# Broker Management API Endpoints
## Register Broker Changes
@app.post("/register_broker_change/")
async def register_broker_change(brokerChange:BrokerChangeRecord):
    brokerChange = brokerChange.dict()
    filePath = "./metadata.json"
    data = load_data(filePath)
    brokers = data["RegisterBrokerRecords"]["records"]

    for index,broker in enumerate(brokers):
        if(broker["brokerId"]==brokerChange["brokerId"]):
            brokers[index] = {**broker,**brokerChange}
            brokers[index]["epoch"] += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data["RegisterBrokerRecords"]["timestamp"] = timestamp
            data["RegistrationChangeBrokerRecord"]["timestamp"] = timestamp
        
            data["RegisterBrokerRecords"]["records"] = brokers
            data["RegistrationChangeBrokerRecord"]["records"].append(brokerChange)
            save_data(filePath,data)
            return "Changes Updated Successfully"
    return "Broker Not Found"
    
## Fetch Changes from last timestamp
@app.post("/metadata_fetch/")
async def metadata_fetch():
    # Logic for metadata fetch
    pass




# Client Management API Endpoints

## Register a producer
@app.post("/register_producer/")
async def register_producer(producerRecord: ProducerIdsRecord):
    filePath = "./metadata.json"
    data = load_data(filePath)
    foundDict = checkProducerExists(producerRecord.dict(),data["ProducerIdsRecord"]["records"])
    if(foundDict):
        return foundDict['producerId']
    serverSetup = producerRecord.dict()
    found_dict = next((producer for producer in data["RegisterBrokerRecords"]["records"] if producer.get("internal_uuid") == serverSetup['brokerId']), None)
    if found_dict:
        serverSetup["brokerEpoch"] = found_dict["epoch"]
        data["ProducerIdsRecord"]["records"].append(serverSetup)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["ProducerIdsRecord"]['timestamp'] = timestamp
        save_data(filePath,data)
        return "Producer Registered Successfully"
    return "Broker Not Recognised"

## Get producer by searchParams -> {partitionId:int, brokerId: str"uuid"}
@app.post("/get_producer/")
async def get_producer(searchParam:SearchParam):
    filePath = "./metadata.json"
    data = load_data(filePath)
    foundDict = checkProducerExists(searchParam.dict(),data["ProducerIdsRecord"]["records"])
    if(foundDict):
        return foundDict
    return "Producer Not Found"

## Get all producers
@app.get("/get_producer/")
async def get_producers():
    filePath = "./metadata.json"
    data = load_data(filePath)
    return data["ProducerIdsRecord"]["records"]



## Fetch Broker, Topic and Partition Records
@app.get("/metadata_fetch_client/")
async def metadata_fetch_client():
    filePath = "./metadata.json"
    data = load_data(filePath)
    returnData = {}
    returnData["RegisterBrokerRecords"] = data["RegisterBrokerRecords"]
    returnData["TopicRecord"] = data["TopicRecord"]
    returnData["PartitionRecord"] = data["PartitionRecord"]
    returnData["RegistrationChangeBrokerRecord"] = data["RegistrationChangeBrokerRecord"]
    return returnData

# Main function to run the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
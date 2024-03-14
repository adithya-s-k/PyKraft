# Utility functions to handle data storage and retrieval
import json


def load_data(path):
    try:
        with open(path, 'r') as file:
            data = json.load(file)
            return data
    except:
        data =  {
            "RegisterBrokerRecords": {
                "records": [],
                "timestamp": ""
            },
            "TopicRecord": {
                "records": [],
                "timestamp": ""
            },
            "PartitionRecord": {
                "records": [],
                "timestamp": ""
            },
            "ProducerIdsRecord": {
                "records": [],
                "timestamp": ""
            },
            "RegistrationChangeBrokerRecord": {
                "records": [],
                "timestamp": ""
            }
        }
        return data

def save_data(path,data):
    with open(path, 'w') as file:
        json.dump(data, file, indent=2)

def checkBrokerExists(brokerData,data):
    found_dict = next((broker for broker in data if broker.get("brokerId") == brokerData['brokerId']), None)
    return found_dict

def checkProducerExists(brokerData,data):
    found_dict = next((broker for broker in data if broker.get("producerId") == brokerData['producerId'] and broker.get("brokerId")==brokerData["brokerId"]), None)
    return found_dict
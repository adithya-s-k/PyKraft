import os
import json
import random
import requests
import time
import sys
import threading
import uuid
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime, timedelta
from schema import *
from utils import *

app = FastAPI()

# Define the base class for Node
class Node:
    def __init__(self, port):
        self.port = port
        self.last_heartbeat_time = None
        self.timeout = 5
        self.heartbeat_interval = 2
        self.current_term = 0
        self.config = self.read_config("config.json")
        self.create_node_files()

    @classmethod
    def initialize_node(cls, port):
        """
        Initialize a Node based on the configuration.
        
        Args:
            port (int): The port number of the node.

        Returns:
            Node: An instance of either Leader or Follower based on the configuration.
        """
        try:
            config = cls.read_config("config.json")
        except:
            config = {"leader_node": None, "follower_nodes": [] , "is_election": False , "term": 0}
            cls.write_config("config.json", config)

        if port == config["leader_node"]:
            return Leader(port)
        else:
            if config["leader_node"] is None:
                config["leader_node"] = port
                cls.write_config("config.json", config)
                return Leader(port)
            else:
                return Follower(port, config["leader_node"])

    def create_node_files(self):
        """
        Create node-specific files and directories.
        """
        os.makedirs(str(self.port), exist_ok=True)
        metadata_template = self.read_metadata_template()
        self.create_or_update_file(f"{self.port}/metadata.json", metadata_template)
        self.create_or_update_file(f"{self.port}/eventlog.json", [])

    def create_or_update_file(self, file_path, data):
        """
        Create or update a file with the provided data.

        Args:
            file_path (str): The path to the file.
            data: The data to write to the file.
        """
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

    @staticmethod
    def read_config(file_path):
        """
        Read the configuration file.

        Args:
            file_path (str): The path to the configuration file.

        Returns:
            dict: The parsed configuration data.
        """
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            sys.exit("Configuration file not found.")
        except json.JSONDecodeError:
            sys.exit("Configuration file is invalid.")

    def read_metadata_template(self):
        """
        Read the metadata schema template.

        Returns:
            dict: The metadata schema template.
        """
        try:
            with open("metajson_schema.json", "r") as file:
                return json.load(file)
        except FileNotFoundError:
            print("Metadata schema file not found.")
            return {}  # Return an empty dict or handle this case as needed
        except json.JSONDecodeError:
            print("Metadata schema file is invalid.")
            return {}  # Return an empty dict or handle this case as needed

    @staticmethod
    def write_config(file_path, data):
        """
        Write data to the configuration file.

        Args:
            file_path (str): The path to the configuration file.
            data: The data to write to the file.
        """
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

    def update_eventlog(self, action, port):
        """
        Update the event log with an action and port information.

        Args:
            action (str): The action to log.
            port (int): The port number associated with the action.
        """
        eventlog = self.read_file(f"{self.port}/eventlog.json")
        eventlog.append({"timestamp": datetime.now().isoformat(), f"port_{action}": port})
        self.create_or_update_file(f"{self.port}/eventlog.json", eventlog)

    def read_file(self, file_path):
        """
        Read data from a file.

        Args:
            file_path (str): The path to the file.

        Returns:
            list: The parsed data from the file.
        """
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return []  # or return a default value appropriate for your application
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
            return []  # or return a default value
        
    def update_eventlog(self, event_type, details):
        """
        Update the event log with various types of events.

        Args:
            event_type (str): The type of event (e.g., 'api_invocation', 'heartbeat_sent').
            details (dict): Detailed information about the event.
        """
        eventlog = self.read_file(f"{self.port}/eventlog.json")
        eventlog.append({"timestamp": datetime.now().isoformat(), "event_type": event_type, "details": details})
        self.create_or_update_file(f"{self.port}/eventlog.json", eventlog)
# Define the Leader class, inheriting from Node
class Leader(Node):
    def __init__(self, port):
        super().__init__(port)
        threading.Thread(target=self.heartbeat_task, daemon=True).start()

    # def send_heartbeat(self, follower_port):
    #     """
    #     Send a heartbeat to a follower.

    #     Args:
    #         follower_port (int): The port of the follower node.
    #     """
    #     metadata = self.read_file(f"{self.port}/metadata.json")
    #     try:
    #         response = requests.post(f"http://localhost:{follower_port}/heartbeat", json=metadata)
    #         print(f"Heartbeat acknowledged by follower on port {follower_port}: {response.json()}")
    #         self.update_eventlog("sent", follower_port)
    #     except requests.RequestException:
    #         print(f"Failed to send heartbeat to follower on port {follower_port}")
    def send_heartbeat(self, follower_port):
        """
        Send a heartbeat to a follower, including the event log.
        """
        metadata = self.read_file(f"{self.port}/metadata.json")
        eventlog = self.read_file(f"{self.port}/eventlog.json")
        heartbeat_payload = {"metadata": metadata, "eventlog": eventlog}
        try:
            response = requests.post(f"http://localhost:{follower_port}/heartbeat", json=heartbeat_payload)
            print(f"Heartbeat acknowledged by follower on port {follower_port}: {response.json()}")
            self.update_eventlog("heartbeat_sent", {"follower_port": follower_port})
        except requests.RequestException:
            print(f"Failed to send heartbeat to follower on port {follower_port}")

    def heartbeat_task(self):
        """
        Periodically send heartbeats to all follower nodes.
        """
        while True:
            threads = []
            for follower_port in self.config["follower_nodes"]:
                thread = threading.Thread(target=self.send_heartbeat, args=(follower_port,))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

            time.sleep(self.heartbeat_interval)

# Define the Follower class, inheriting from Node
class Follower(Node):
    def __init__(self, port, leader_port):
        super().__init__(port)
        self.register_with_leader(leader_port)
        self.ack_lock = threading.Lock()
        self.acknowledgements = 0
        threading.Thread(target=self.monitor_heartbeat, daemon=True).start()

    def register_with_leader(self, leader_port):
        """
        Register as a follower with the leader.

        Args:
            leader_port (int): The port of the leader node.
        """
        url = f"http://localhost:{leader_port}/register_follower"
        response = requests.post(url, json={"follower_port": self.port})
        if response.status_code != 200:
            print(f"Error registering with leader: {response.content}")
            

    def send_request_to_followers(self):
        threads = []
        config = self.read_config("config.json")
        total_followers = len(config["follower_nodes"])

        for follower_port in config["follower_nodes"]:
            thread = threading.Thread(target=self.send_request_to_follower, args=(follower_port,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        return total_followers, self.acknowledgements

    def send_request_to_follower(self, follower_port):
        url = f"http://localhost:{follower_port}/set_leader/{self.port}"
        response = requests.post(url)
        if response.status_code == 200:
            with self.ack_lock:
                print(f"Vote received from {follower_port}")
                self.acknowledgements += 1
        else:
            print(f"did not receive vote from {follower_port}")
            print(f"Error registering with leader: {response.content}")
            self.achnowledgements += 0

    def monitor_heartbeat(self):
        """
        Monitor the leader's heartbeat and check for leader failure.
        """
        while True:
            time.sleep(1)
            if self.last_heartbeat_time and datetime.now() - self.last_heartbeat_time > timedelta(seconds=self.timeout):
                print("Leader is dead")
                config = self.read_config("config.json")
                config["term"] += 1
                self.write_config("config.json", config)
                break
        self.initiate_leader_election()

    def initiate_leader_election(self):
        global node  # Reference the global node variable

        config = self.read_config("config.json")
        config["is_election"] = True
        self.write_config("config.json", config)

        random_shutdown_delay = random.randint(1, 10)  # Random delay between 1 to 10 seconds
        print(f"Becoming candidate in {random_shutdown_delay} seconds...")
        time.sleep(random_shutdown_delay)

        config = self.read_config("config.json")
        if config["is_election"]:
            config["is_election"] = False
            self.write_config("config.json", config)
            print("I am a candidate now")
            config["leader_node"] = None
            # Remove the leader node port from the follower nodes
            config["follower_nodes"] = [p for p in config["follower_nodes"] if int(p) != self.port]
            self.write_config("config.json", config)
            #send request to all followers to vote for simultaneously using threads
            print("Sending request to all followers to vote for me")
            total_followers, acknowledgements= self.send_request_to_followers()
            print("total followers", total_followers)
            print("acknowledgements", acknowledgements)
            if acknowledgements >= total_followers/2:
                print("I am a leader now")
                config["is_election"] = False
                config["leader_node"] = self.port
                self.write_config("config.json", config)
                node = Leader(self.port)
            else:
                self.initiate_leader_election()

# FastAPI endpoints
@app.post("/register_follower")
async def register_follower(follower_data: FollowerRegistration, background_tasks: BackgroundTasks):
    """
    Endpoint to register a follower with the leader node.

    Args:
        follower_data (FollowerRegistration): Data for registering a follower.
        background_tasks (BackgroundTasks): Background tasks to send a heartbeat to the new follower.

    Returns:
        dict: A message indicating the successful registration of the follower.
    """
    if isinstance(node, Leader):
        node.config = node.read_config("config.json")
        new_follower_port = follower_data.follower_port

        if new_follower_port not in node.config["follower_nodes"]:
            node.config["follower_nodes"].append(new_follower_port)
            node.write_config("config.json", node.config)
            node.update_eventlog("api_invocation", {"endpoint": "register_follower", "data": follower_data.dict()})

            # Add the send_heartbeat task only if the node is a Leader
            if isinstance(node, Leader):
                background_tasks.add_task(node.send_heartbeat, new_follower_port)
        return {"message": "Follower registered"}
    else:
        return {"message": "Not a leader node"}

# @app.post("/heartbeat")
# def heartbeat(metadata: dict):
#     """
#     Endpoint to handle heartbeats from followers.

#     Args:
#         metadata (dict): Metadata received in the heartbeat request.

#     Returns:
#         dict: A message indicating acknowledgment.
#     """
#     if isinstance(node, Follower):
#         with threading.Lock():
#             node.last_heartbeat_time = datetime.now()
#             node.create_or_update_file(f"{node.port}/metadata.json", metadata)
#             node.update_eventlog("received", node.port)
#     return {"message": "Acknowledged"}

@app.post("/heartbeat")
def heartbeat(heartbeat_payload: dict):
    """
    Endpoint to handle heartbeats from the leader, which includes metadata and event log.

    Args:
        heartbeat_payload (dict): Payload received in the heartbeat request, including metadata and event log.

    Returns:
        dict: A message indicating acknowledgment.
    """
    if isinstance(node, Follower):
        with threading.Lock():
            # Update last heartbeat time
            node.last_heartbeat_time = datetime.now()

            # Update metadata and event log
            if "metadata" in heartbeat_payload:
                node.create_or_update_file(f"{node.port}/metadata.json", heartbeat_payload["metadata"])
            if "eventlog" in heartbeat_payload:
                node.create_or_update_file(f"{node.port}/eventlog.json", heartbeat_payload["eventlog"])
            
            # Log the receipt of the heartbeat
            node.update_eventlog("heartbeat_received", {"from_port": node.port})

    return {"message": "Acknowledged"}

@app.post("/set_leader/{port_number}")
def set_leader(port_number: int):
    global node
    if isinstance(node, Follower):
        node.update_eventlog("api_invocation", {"endpoint": "set_leader", "port_number": port_number})
        print(f"Sending vote to {port_number} from {node.port}")
        print(f"I am a follower now and my leader is {port_number}")
        node = Follower(node.port, port_number)
    return {"message": f"Vote from {node.port}"}




@app.post("/register_broker/")
async def register_broker(broker: BrokerRecord):
    """
    Registers a new broker in the system.

    Args:
        broker (BrokerRecord): The broker information for registration.

    Returns:
        str: The internal UUID of the newly registered broker.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "register_broker", "broker": broker.dict()})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}


## Get all brokers
@app.get("/get_broker/")
async def get_allbrokers():
    """
    Retrieves a list of all registered brokers.

    Returns:
        list: A list of registered broker records.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "get_allbrokers"})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        return data["RegisterBrokerRecords"]["records"]
    else:
        return {"message": "Not a leader node"}

## Get broker by id
@app.get("/get_broker/{broker_id}")
async def get_broker_by_ID(broker_id:int):
    """
    Retrieves a specific broker by its ID.

    Args:
        broker_id (int): The ID of the broker to retrieve.

    Returns:
        dict: The broker record if found, otherwise a 'Broker Not Found' message.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "get_broker_by_ID", "broker_id": broker_id})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        found_dict = next((broker for broker in data["RegisterBrokerRecords"]["records"] if broker.get("brokerId") == broker_id), None)
        if found_dict:
            return found_dict
        return "Broker Not Found"
    else:
        return {"message": "Not a leader node"}

## Delete broker by id
@app.delete("/delete_broker/{broker_id}")
async def delete_broker(broker_id: int):
    """
    Deletes a broker from the system by its ID.

    Args:
        broker_id (int): The ID of the broker to delete.

    Returns:
        dict: The deleted broker record or a 'Broker Not Found' message if not found.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "delete_broker", "broker_id": broker_id})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}
## Register Topic
@app.post("/register_topic/")
async def register_topic(topicRecord:TopicRecord):
    """
    Registers a new topic in the system.

    Args:
        topicRecord (TopicRecord): The topic information for registration.

    Returns:
        str: The UUID of the newly registered topic.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "register_topic", "topic_record": topicRecord.dict()})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}

## Get Topic by topic name
@app.get("/get_topic/{topicName}")
async def getTopicByName(topicName:str):
    """
    Retrieves a specific topic by its name.

    Args:
        topicName (str): The name of the topic to retrieve.

    Returns:
        dict: The topic record if found, otherwise a 'Topic Not Found' message.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "getTopicByName", "topic_name": topicName})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        found_dict = next((topic for topic in data["TopicRecord"]["records"] if topic.get("name") == topicName), None)
        if(found_dict):
            return found_dict
        return "Topic Not Found"
    else:
        return {"message": "Not a leader node"}

## Get all topics 
@app.get("/get_topic/")
async def getAllTopics():
    """
    Retrieves a list of all registered topics.

    Returns:
        list: A list of registered topic records.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "getAllTopics"})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        return data["TopicRecord"]["records"]
    else:
        return {"message": "Not a leader node"}

## Delete topic by topic name
@app.delete("/delete_topic/{topicName}")
async def delete_topicByName(topicName: str):
    """
    Deletes a topic from the system by its name.

    Args:
        topicName (str): The name of the topic to delete.

    Returns:
        dict: The deleted topic record or a 'Topic Not Found' message if not found.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "delete_topicByName", "topic_name": topicName})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}

## Register a partition
@app.post("/register_partition/")
async def register_partition(partitionRecord:PartitionRecord):
    """
    Registers a new partition in the system.

    Args:
        partitionRecord (PartitionRecord): The partition information for registration.

    Returns:
        str: The UUID of the newly registered partition.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "register_partition", "partition_record": partitionRecord.dict()})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}

## Get partition by prtitionId
@app.get("/get_partition/{partitionId}")
async def get_partitionByID(partitionId:int):
    """
    Retrieves a specific partition by its ID.

    Args:
        partitionId (int): The ID of the partition to retrieve.

    Returns:
        dict: The partition record if found, otherwise a 'Partition Not Found' message.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "get_partitionByID", "partition_id": partitionId})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        found_dict = next((partition for partition in data["PartitionRecord"]["records"] if partition.get("partitionId") == partitionId), None)
        if(found_dict):
            return found_dict
        return "Partition Not Found"
    else:
        return {"message": "Not a leader node"}

## Get all partitions
@app.get("/get_partition/")
async def get_allpartitions():
    """
    Retrieves a list of all registered partitions.

    Returns:
        list: A list of registered partition records.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "get_allpartitions"})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        return data["PartitionRecord"]["records"]
    else:
        return {"message": "Not a leader node"}

## Delete a partition by partitionId
@app.delete("/delete_partition/{partition_id}")
async def delete_partition(partition_id: int):
    """
    Deletes a partition from the system by its ID.

    Args:
        partition_id (int): The ID of the partition to delete.

    Returns:
        dict: The deleted partition record or a 'Partition Not Found' message if not found.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "delete_partition", "partition_id": partition_id})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}
    
# Broker Management API Endpoints
## Register Broker Changes
@app.post("/register_broker_change/")
async def register_broker_change(brokerChange:BrokerChangeRecord):
    """
    Registers changes to an existing broker.

    Args:
        brokerChange (BrokerChangeRecord): The broker change information.

    Returns:
        str: A message indicating successful update or broker not found.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "register_broker_change", "broker_change": brokerChange.dict()})
        brokerChange = brokerChange.dict()
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}

## Fetch Changes from last timestamp
@app.post("/metadata_fetch/")
async def metadata_fetch():
    """
    Fetches metadata changes since the last timestamp. 
    (Note: Implementation logic is to be defined)
    
    Returns:
        TBD: Based on the implementation details.
    """
    # Logic for metadata fetch
    node.update_eventlog("api_invocation", {"endpoint": "metadata_fetch"})
    pass

# Client Management API Endpoints

## Register a producer
@app.post("/register_producer/")
async def register_producer(producerRecord: ProducerIdsRecord):
    """
    Registers a new producer in the system.

    Args:
        producerRecord (ProducerIdsRecord): The producer information for registration.

    Returns:
        str: A message indicating successful registration or if the broker is not recognized.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "register_producer", "producer_record": producerRecord.dict()})
        filePath = f"{node.port}/metadata.json"
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
    else:
        return {"message": "Not a leader node"}

## Get producer by searchParams -> {partitionId:int, brokerId: str"uuid"}
@app.post("/get_producer/")
async def get_producer(searchParam:SearchParam):
    """
    Retrieves a specific producer based on search parameters.

    Args:
        searchParam (SearchParam): Parameters to search for a specific producer.

    Returns:
        dict: The producer record if found, otherwise a 'Producer Not Found' message.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "get_producer", "search_param": searchParam.dict()})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        foundDict = checkProducerExists(searchParam.dict(),data["ProducerIdsRecord"]["records"])
        if(foundDict):
            return foundDict
        return "Producer Not Found"
    else:
        return {"message": "Not a leader node"}

## Get all producers
@app.get("/get_producer/")
async def get_producers():
    """
    Retrieves a list of all registered producers.

    Returns:
        list: A list of registered producer records.
    """
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "get_producers"})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        return data["ProducerIdsRecord"]["records"]
    else:
        return {"message": "Not a leader node"}

## Fetch Broker, Topic and Partition Records
@app.get("/metadata_fetch_client/")
async def metadata_fetch_client():
    if isinstance(node, Leader):
        node.update_eventlog("api_invocation", {"endpoint": "metadata_fetch_client"})
        filePath = f"{node.port}/metadata.json"
        data = load_data(filePath)
        returnData = {}
        returnData["RegisterBrokerRecords"] = data["RegisterBrokerRecords"]
        returnData["TopicRecord"] = data["TopicRecord"]
        returnData["PartitionRecord"] = data["PartitionRecord"]
        returnData["RegistrationChangeBrokerRecord"] = data["RegistrationChangeBrokerRecord"]
        return returnData
    else:
        return {"message": "Not a leader node"}

if __name__ == "__main__":
    port = int(sys.argv[1])
    node = Node.initialize_node(port)

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
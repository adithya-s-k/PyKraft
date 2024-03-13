from pydantic import BaseModel
from typing import List

class BrokerRecord(BaseModel):
    brokerId: int
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    rackId:str

class BrokerChangeRecord(BaseModel):
    brokerId: int # ?? Changed it to int <-----
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    brokerStatus:str

class TopicRecord(BaseModel):
    name: str

class PartitionRecord(BaseModel):
    partitionId: int
    topicUUID: str
    replicas: List[str]
    ISR: List[int]
    leader: str
    removingReplicas: List[int]
    addingReplicas: List[int]
    partitionEpoch: int


class ProducerIdsRecord(BaseModel):
    brokerId: str
    brokerEpoch: int
    producerId: int

class SearchParam(BaseModel):
    brokerId: str
    producerId: int
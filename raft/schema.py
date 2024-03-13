from pydantic import BaseModel
from typing import List

class LeaderInformation(BaseModel):
    new_leader_port: int

class FollowerRegistration(BaseModel):
    follower_port: int
    
class VoteRequest(BaseModel):
    candidate_port: int
    # term: int

class LeaderData(BaseModel):
    leader_port: int

class BrokerRecord(BaseModel):
    brokerId: int
    brokerHost:str
    brokerPort:int
    securityProtocol:str
    rackId:str
    

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
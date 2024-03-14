# CRD API

## Create

#### Endpoint: `/register_broker/`

- **Method**: POST
- **Description**: Creates a new record under RegisterBrokerRecords.
- **Request Body**:

```
{
	"brokerId": 5,
	"brokerHost": "127.0.0.1",
	"brokerPort": 8005,
	"securityProtocol": "",
	"rackId": "",
}
```

- **Response**: Returns `internal_uuid` of the created record.

#### Endpoint: `/register_topic/`

- **Method**: POST
- **Description**: Creates a new record under TopicRecord.
- **Request Body**:

```
{
	"name": "Topic Name -1"
}
```

- **Response**: Returns `topicuuid` of the created record.

#### Endpoint: `/register_partition/`

- **Method**: POST
- **Description**: Creates a new record under PartitionRecord.
- **Request Body**:

```
{
		"partitionId": 0,
		"topicUUID": "",
		"replicas": [],
		"ISR": [],
		"removingReplicas": [],
		"addingReplicas": [],
		"leader": "",
		"partitionEpoch": 0
	}
```

- **Response**: Returns `topicuuid` of the created record.

## Read

#### **Endpoint**: `/get_broker/`

- **Method**: GET
- **Description**: Retrieves single or multiple records of brokers.
- **Query Parameters**: `broker_id` (\*Optional).
- **Response**: JSON array of records / Single requested record.

#### **Endpoint**: `/get_topic/`

- **Method**: GET
- **Description**: Retrieves single or multiple records of topics.
- **Query Parameters**: `topic_name` (\*Optional).
- **Response**: JSON array of records / Single requested record.

#### **Endpoint**: `/get_partition/`

- **Method**: GET
- **Description**: Retrieves single or multiple records of partitions.
- **Query Parameters**: `partition_id` (\*Optional).
- **Response**: JSON array of records / Single requested record.

#### **Endpoint**: `/get_producer/`

- **Method**: GET (to retrieve all records) / POST (to retrieve selected record)
- **Description**: Retrieves single or multiple records of producers.
- **Request Body**: `{partitionId: 0, brokerId: "uuid"}` (\*Optional).
- **Response**: JSON array of records / Single requested record.

## Delete

#### **Endpoint**: `/delete_broker/{broker_id}`

- **Method**: DELETE
- **Description**: Deletes a specific record by its unique ID.
- **Path Parameter**: `id` - Unique ID of the entity to delete.
- **Response**: Deleted Record.

#### **Endpoint**: `/delete_topic/{topicName}`

- **Method**: DELETE
- **Description**: Deletes a specific record by its unique ID.
- **Path Parameter**: `id` - Unique ID of the entity to delete.
- **Response**: Deleted Record.

#### **Endpoint**: `/delete_partition/{partition_id}`

- **Method**: DELETE
- **Description**: Deletes a specific record by its unique ID.
- **Path Parameter**: `id` - Unique ID of the entity to delete.
- **Response**: Deleted Record.

## Broker Management API

### RegisterBroker

#### Endpoint: `/register_broker/`

- **Method**: POST
- **Description**: Creates a new record under RegisterBrokerRecords.
- **Request Body**:

```
{
	"brokerId": 5,
	"brokerHost": "127.0.0.1",
	"brokerPort": 8005,
	"securityProtocol": "",
	"rackId": "",
}
```

- **Response**: Returns `internal_uuid` of the created record.

### RegisterBrokerChange

#### Endpoint: `/register_broker_change/`

- **Method**: POST
- **Description**: Changes specified record in `RegisterBrokerRecord`.
- **Request Body**:

```
{
    "brokerId": 5,
    "brokerHost": "127.0.0.1",
    "brokerPort": "8005",
    "securityProtocol": "",
    "brokerStatus":"INIT"
}
```

- **Response**: Acknowledgement of change.

### MetadataFetch

#### To be implemented

## Client Management API

### RegisterProducer

- **Endpoint**: `/register_producer/`
- **Method**: POST
- **Description**: Registers a new producer in the system.
- **Request Body**:

```
{
    "brokerId":"5",
    "producerId":0,
    "brokerEpoch":0
}
```

- **Response**: Acknowledgment of registration.

### MetadataFetch

- **Endpoint**: `/metadata_fetch_client/`
- **Method**: GET
- **Description**: Fetches topics, partition, and broker information records for clients.
- **Response**: Requested metadata information.

### To Start the Server:

```
uvicorn main:app --reload
```

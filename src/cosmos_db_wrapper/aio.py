"""Front-end helper functions to access Cosmos DB (async)
"""
import logging
from datetime import datetime

from azure.cosmos import PartitionKey, exceptions
from azure.cosmos.aio import CosmosClient
from azure.cosmos.aio._database import DatabaseProxy
from azure.cosmos.aio._container import ContainerProxy


async def get_or_create_db(client: CosmosClient, database_name: str) -> DatabaseProxy:
    try:
        database_proxy = client.create_database(id=database_name)
        logging.debug(F"Creating database {database_name}")
    except exceptions.CosmosResourceExistsError:
        database_proxy = client.get_database_client(database=database_name)
        logging.debug(F"Database {database_name} already exists")

    return await database_proxy


async def get_or_create_container(database_proxy: DatabaseProxy, container_name: str, partition_key_path: str) -> ContainerProxy:
    try:
        container_proxy = database_proxy.create_container(
            id=container_name, partition_key=PartitionKey(
                path=partition_key_path)
        )
        logging.debug(
            F"Creating container {container_name} with parition key {partition_key_path}")
    except exceptions.CosmosResourceExistsError:
        container_proxy = database_proxy.get_container_client(container_name)
        logging.debug(F"Container {container_name} already exists")

    return await container_proxy


async def query_items(container_proxy: ContainerProxy, query_text: str):
    query_items_response = container_proxy.query_items(
        query=query_text,
        enable_cross_partition_query=True)

    items = [item async for item in query_items_response]
    logging.debug(F"async.query_items()-type(items) =  {type(items)}")

    return items

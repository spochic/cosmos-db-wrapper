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
        logging.debug(
            F"get_or_create_db()-Trying to create database '{database_name}'")
        database_proxy = await client.create_database(id=database_name)
        logging.debug(F"get_or_create_db()-Database '{database_name}' created")

    except exceptions.CosmosResourceExistsError:
        logging.debug(
            F"get_or_create_db()-Database '{database_name}' already exists")
        database_proxy = await client.get_database_client(database=database_name)

    finally:
        logging.debug(
            F"get_or_create_db()-Returning proxy to '{database_name}' database")
        return database_proxy


async def get_or_create_container(database_proxy: DatabaseProxy, container_name: str, partition_key_path: str) -> ContainerProxy:
    try:
        logging.debug(
            F"get_or_create_container()-Trying to create container '{container_name}'")
        container_proxy = await database_proxy.create_container(
            id=container_name, partition_key=PartitionKey(
                path=partition_key_path)
        )
        logging.debug(
            F"get_or_create_container()-Container '{container_name}' created")

    except exceptions.CosmosResourceExistsError:
        logging.debug(
            F"get_or_create_container()-Container '{container_name}' already exists")
        container_proxy = await database_proxy.get_container_client(container_name)

    finally:
        logging.debug(
            F"get_or_create_container()-Returning proxy to '{container_name}' container")
        return container_proxy


async def query_items(container_proxy: ContainerProxy, query_text: str):
    query_items_response = container_proxy.query_items(
        query=query_text,
        enable_cross_partition_query=True)

    items = [item async for item in query_items_response]
    logging.debug(F"async.query_items()-type(items) =  {type(items)}")

    return items

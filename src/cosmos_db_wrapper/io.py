"""Front-end helper functions to access Cosmos DB (synchronous)
"""
import logging
from datetime import datetime

from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.cosmos.database import DatabaseProxy, ContainerProxy


def get_or_create_db(client: CosmosClient, database_name: str) -> DatabaseProxy:
    try:
        logging.debug(
            F"get_or_create_db()-Trying to create database '{database_name}'")
        database_proxy = client.create_database(id=database_name)
        logging.debug(F"get_or_create_db()-Database '{database_name}' created")

    except exceptions.CosmosResourceExistsError:
        logging.debug(
            F"get_or_create_db()-Database '{database_name}' already exists")
        database_proxy = client.get_database_client(database=database_name)

    finally:
        logging.debug(
            F"get_or_create_db()-Returning proxy to '{database_name}' database")
        return database_proxy


def get_or_create_container(database_proxy: DatabaseProxy, container_name: str, partition_key_path: str) -> ContainerProxy:
    try:
        logging.debug(
            F"get_or_create_container()-Trying to create container '{container_name}'")
        container_proxy = database_proxy.create_container(
            id=container_name, partition_key=PartitionKey(
                path=partition_key_path)
        )
        logging.debug(
            F"get_or_create_container()-Container '{container_name}' created")

    except exceptions.CosmosResourceExistsError:
        logging.debug(
            F"get_or_create_container()-Container '{container_name}' already exists")
        container_proxy = database_proxy.get_container_client(container_name)

    finally:
        logging.debug(
            F"get_or_create_container()-Returning proxy to '{container_name}' container")
        return container_proxy


def query_items(container_proxy: ContainerProxy, query_text: str):
    logging.debug(F"query_items()-query_text =  {query_text}")
    query_items_response = container_proxy.query_items(
        query=query_text,
        enable_cross_partition_query=True)

    logging.debug(F"query_items()-type(items) =  {type(query_items_response)}")
    logging.debug(F"query_items()-items =  {query_items_response}")

    return query_items_response


def read_all_items(container_proxy: ContainerProxy):
    try:
        logging.debug(F"read_all_items()")
        items = list(container_proxy.read_all_items())
        logging.debug(F"read_all_items()-len(items) = {len(items)}")
        return items

    except exceptions.CosmosResourceNotFoundError:
        return []


def read_item(container_proxy: ContainerProxy, item_id: str, partition_key: str):
    try:
        logging.debug(F"read_item()-item_id = {item_id}")
        item = container_proxy.read_item(item_id, partition_key)
        logging.debug(F"read_item()-item = {item}")
        return item

    except exceptions.CosmosResourceNotFoundError:
        return {}


def get_item_by_uri(container_proxy: ContainerProxy, uri: str):
    logging.debug(F"get_item_by_uri()-uri = {uri}")
    items = list(query_items(
        container_proxy,
        F"SELECT * FROM c WHERE c.uri = '{uri}'"))
    logging.debug(F"get_item_by_uri()-items = {items}")
    logging.debug(F"get_item_by_uri()-len(items) = {len(items)}")
    assert len(items) <= 1

    if len(items) == 0:
        return {}
    else:
        logging.debug(F"get_item_by_uri()-type(items[0]) =  {type(items[0])}")
        return items[0]


def create_item(container_proxy: ContainerProxy, item: str):
    logging.debug(F"create_item()-item = {item}")
    upserted_item = container_proxy.upsert_item(body=item)
    logging.debug(F"create_item()-upserted item = {upserted_item}")

    return upserted_item

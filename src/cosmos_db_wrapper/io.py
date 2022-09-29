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
    logging.debug(F"read_all_items()")
    items = container_proxy.read_all_items()
    logging.debug(F"read_all_items()-len(items) = {len(items)}")

    return items


def get_item_by_id(container_proxy: ContainerProxy, item_id: str):
    logging.debug(F"get_item_by_id()-item_id = {item_id}")
    items = query_items(
        container_proxy,
        F"SELECT * FROM c WHERE c.id = '{item_id}'")
    logging.debug(F"get_item_by_id()-items = {items}")
    logging.debug(F"get_item_by_id()-len(items) = {len(items)}")
    assert len(items) <= 1

    if len(items) == 0:
        return None
    else:
        return items[0]


def create_item(container_proxy: ContainerProxy, item: str):
    item['last_update'] = datetime.utcnow().isoformat()
    logging.debug(F"create_item()-item = {item}")
    upserted_item = container_proxy.upsert_item(body=item)
    logging.debug(F"create_item()-inserted item = {upserted_item}")

    return upserted_item

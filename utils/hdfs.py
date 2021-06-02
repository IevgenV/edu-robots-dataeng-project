import logging
import pathlib

from hdfs.client import Client


class HDFSDefaults:
    DEFAULT_BRONZE_PATH = "/bronze"
    DEFAULT_SILVER_PATH = "/silver"


def create_hdfs_path_if_not_exists(hdfs_client:Client
                                 , data_path:pathlib.Path) -> bool:
    assert hdfs_client is not None, "HDFS Client can't be None"
    if hdfs_client.status(data_path, strict=False) is None:
        try:
            logging.info(f"{data_path} directory doesn't exist. Creating directory tree...")
            hdfs_client.makedirs(data_path)
            logging.info(f"{data_path} directory tree has been created.")
            return True
        except OSError:
            logging.error(f"{data_path} directory tree can not be created.")
            raise OSError("Can't create HDFS data directory: '{}'".format(data_path))
    return False

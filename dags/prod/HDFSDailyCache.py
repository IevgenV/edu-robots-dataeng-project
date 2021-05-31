import json
import pathlib
import shutil
from datetime import date
from typing import Optional

from hdfs import InsecureClient
from hdfs import Client

from .FileDailyCache import FileDailyCache
from .Server import DailyDataSource


class HDFSDailyCache(FileDailyCache):

    def __init__(self, hdfs_client:Client, cache_dir:pathlib.Path, data_source:DailyDataSource, cache_date:date=None):
        assert isinstance(hdfs_client, Client)
        self.client = hdfs_client
        super(HDFSDailyCache, self).__init__(cache_dir, data_source, cache_date)

    def get_data(self) -> Optional[dict]:
        cache_filepath = self.get_cache_filepath()
        if self.client.status(cache_filepath, strict=False) is None:
            return None  # <- Data for cache day is not available
        with self.client.read(cache_filepath, encoding="utf-8") as cache_file:
            return {self.cache_date: json.load(cache_file)}

    def save_data_to_file(self, data:dict) -> None:
        cache_filepath = self.get_cache_filepath()
        cache_subdir = cache_filepath.parents[0]  # <- Get directory where file is located
        if self.client.status(cache_subdir, strict=False) is None:
            try:
                self.client.makedirs(cache_subdir)
            except OSError:
                raise OSError("Can't create HDFS data directory: '{}'".format(cache_subdir))
        with self.client.write(cache_filepath, overwrite=True, encoding="utf-8") as cache_file:
            json.dump(data, cache_file)

    def clear(self) -> bool:
        if self.client.status(self.cache_dir, strict=False) is not None:
            self.client.delete(self.cache_dir, recursive=True)

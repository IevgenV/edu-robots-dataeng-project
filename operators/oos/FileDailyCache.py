import json
import pathlib
import shutil
from datetime import date
from typing import Optional

from .Cache import Cache
from .Server import DailyDataSource


class FileDailyCache(Cache):

    def __init__(self, cache_dir:pathlib.Path, data_source:DailyDataSource, cache_date:date=None):
        assert isinstance(cache_dir, pathlib.Path)
        assert isinstance(data_source, DailyDataSource)
        cache_date = date.today() if cache_date is None else cache_date
        assert isinstance(cache_date, date)

        self.cache_dir = cache_dir
        self.data_source = data_source
        self.cache_date = cache_date

    def get_data(self) -> Optional[dict]:
        cache_filepath = self.get_cache_filepath()
        if not cache_filepath.exists() and not self.update():
            return None  # <- Data for cache day is not available
        with open(cache_filepath, 'r') as cache_file:
            return {self.cache_date: json.load(cache_file)}

    def update(self) -> bool:
        new_data = self.data_source.get_data_by_date(self.cache_date)
        if self.cache_date not in new_data:
            return False
        self.save_data_to_file(new_data[self.cache_date])
        return True

    def set_cache_date(self, cache_date:date):
        assert isinstance(cache_date, date)
        self.cache_date = cache_date

    def save_data_to_file(self, data:dict) -> None:
        cache_filepath = self.get_cache_filepath()
        cache_subdir = cache_filepath.parents[0]  # <- Get directory where file is located
        cache_subdir.mkdir(parents=True, exist_ok=True)
        with open(cache_filepath, 'w') as cache_file:
            json.dump(data, cache_file)

    def clear(self) -> bool:
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir, ignore_errors=True)

    def get_cache_filepath(self) -> pathlib.Path:
        day_str = self.cache_date.isoformat()  # ISO: "YYYY-MM-DD"
        cache_subdir = self.cache_dir / pathlib.Path(day_str)
        cache_filepath = cache_subdir / pathlib.Path(f"{day_str}.json")
        return cache_filepath

import abc
import json
import pathlib
from datetime import date, timedelta

import requests
import yaml

from typing import Optional, Union


class DailyDataSource(metaclass=abc.ABCMeta):
    """ Abstraction allowing getting data by dates using custom datasources. """

    @abc.abstractmethod
    def get_data_by_date(self, date_start:date, date_end:Optional[date]=None) -> dict:
        """ Retrieves the data from custom source by the specific date or range of dates.

        Args:
            date_start (date): The date for which data is requested from the source. If `date_end`
                               is also specified, then data will be requested for the period from
                               `date_start` to `date_end`.
            date_end (Optional[date]): Optional argument. If it is specified, then the period from
                               `date_start` to `date_end` is used for data retrieval. If it is set
                               to None, only `date_start` is used. Defaults to None.

        Returns:
            dict: The dictionary where dates are keys and data is values gotten from data source.
        """
        pass


class ProdServer(DailyDataSource):
    
    def __init__(self, configuration:Union[pathlib.Path, dict], server_name:str="prod_server"):
        assert isinstance(configuration, pathlib.Path) or isinstance(configuration, dict), \
                "Configuration must be specified with a pathlib.Path or be a dictionary."
        assert isinstance(server_name, str)

        if isinstance(configuration, dict):
            cfg = configuration
        else:
            with open(configuration) as cfg_fp:
                cfg = yaml.safe_load(cfg_fp)

        self.name = server_name

        self.login = cfg[server_name]["login"]
        self.passwd = cfg[server_name]["password"]

        self.address = cfg[server_name]["address"]
        self.authorize_api = cfg[server_name]["apis"]["authorize"]
        self.out_of_stock_api = cfg[server_name]["apis"]["out_of_stock"]

        self.token = self.__request_token()

    def get_data_by_date(self, date_start:date, date_end:Optional[date]=None) -> dict:
        date_end = date_start if date_end is None else date_end
        assert date_start <= date_end, "start_date has to be less or equal to end_date"

        url = self.address + self.out_of_stock_api["endpoint"]
        headers = {"content-type": "application/json", "Authorization": self.token}

        products = {}
        request_date = date_start
        request_attempts_left = 5  # If server responces error, 5 additional attempts are allowed

        while request_date <= date_end:
            data = {"date": request_date.isoformat()}  # ISO: "YYYY-MM-DD"
            r = requests.get(url, data=json.dumps(data), headers=headers)

            if r.status_code == 401 and request_attempts_left > 0:  # Unauthorized Error
                headers["Authorization"] = self.__request_token()
                request_attempts_left -= 1
                continue
            elif r.status_code == 404:
                if r.json().get("message") == "No out_of_stock items for this date":
                    request_date += timedelta(1)
                    continue

            r.raise_for_status()
            products[request_date] = r.json()
            request_date += timedelta(1)

        return products

    def __request_token(self) -> str:
        url = self.address + self.authorize_api["endpoint"]
        headers = {"content-type": "application/json"}
        data = {"username": self.login, "password": self.passwd}
        r = requests.post(url, data=json.dumps(data), headers=headers)
        r.raise_for_status()
        r_json = r.json()
        self.token = "JWT " + r_json["access_token"]
        return self.token

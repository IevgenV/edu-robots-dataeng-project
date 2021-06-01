import abc
from typing import Optional


class Cache(metaclass=abc.ABCMeta):
    """ Interface for any cache object providing `get_data()` and `update()` functionalities only. """

    @abc.abstractmethod
    def get_data(self) -> Optional[dict]:
        """ Returns the data.
        
        If data is already cached, it will be retuened from cache. In other case, the data will be
        requested from data source first. Subsequent calls will return cached values. For updating
        the cache, 'update()' method should be used.

            Optional[dict]: Data dictionary. Keys of the dictionary are used to determine whether
                            the data was cached or not. I.e., existed in dictionary keys identify
                            cached data. The more data is cached, the more keys are available. If
                            None is returned, then data is not availbale nor from cache, nor from
                            data source.
        """
        pass

    @abc.abstractmethod
    def update(self) -> bool:
        """ Updates the data in cache.

        Update of the data will be forced even if data is already cached. `update()` will forcibly
        request the data source for the data ignoring the current cache.

        Returns:
            bool: True if the data source returned data to be cached. False otherwise. The latter
                  means that cache isn't updated.
        """
        pass

    def clear(self) -> bool:
        """ Clears all the cache. It will delete all the cahce. So, use carefully.

        Returns:
            bool: True if the cache was existed before the clear. False otherwise.
        """
        pass

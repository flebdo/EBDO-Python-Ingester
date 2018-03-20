# Copyright (C) 2018 Project-EBDO
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# EBDO-Ingester
# Author: Flebdo

"""
Elasticsearch writer

Parameters:
    - host: hostname or ip address of ES instance
    - port: port of ES API
    - index: elasticsearch index where data will be imported

Methods:
    - write:
        - data: (dict) import data to ES to the index defined in parameters
    - close: do nothing (present to satisfy required methods to be a valid writer)
"""

import logging as log


# Custom ESWriter exceptions
class ESmoduleNotInstalled(Exception):
    """
    Elasticsearch module for python 3 is not installed
    (or the install is broken)
    """
    pass


class ESnotReachable(Exception):
    """
    Elasticsearch is not accessible.
    """
    pass

class ESimportFailed(Exception):
    """
    Failed to import data into Elasticsearch.
    """
    pass


# Try to import ES module (it is not a standard python module),
# show instructions if it is not installed
try:
    from elasticsearch import Elasticsearch
except ImportError:
    log.error("Elasticsearch module for python is not installed.")
    log.error("It is required to use elasticsearch backend.")
    log.error("Try: pip3 install --user elasticsearch")
    raise ESmoduleNotInstalled


class ESWriter():

    def __init__(self, host, port, index):

        # Create ES objet
        self.__es = Elasticsearch([
                                  {'host': host, 'port': port}
                                  ])
        self.__es_index = index

        if not self.__es.ping():
            log.error("Elasticsearch is not reachable")
            log.error("Check host and port.")
            raise ESnotReachable

    def write(self, data):
        try:
            self.__es.index(index=self.__es_index,
                            doc_type="ode_data",
                            body=data
                            )
        except Exception:
            log.exception("Error while importing data to ES")
            log.error("data : " + str(data))
            raise ESimportFailed

    def close(self):
        # No explicit way to close ES socket (AFAIK)
        pass

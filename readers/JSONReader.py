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

import logging as log
import json


# Custom JSON exceptions
class ValueNameNotFoundInJSONFile(Exception):
    """
    A value name set in config file was not found in JSON file
    """
    pass


class JSONReader():
    """
    This reader is iterable and expects
    *one valid json per line*.

    Parameters:
        - fd: (fd) file descriptor of input file
        - inputValueNames: (tuple or list of str) Column names specified in config file

    Return:
        data():
            - an iterable object,
              each iteration returns a dictionary {valueName: value, ...}
    """

    def __init__(self, fd, inputValueNames):

        # Store fd
        self.__fd = fd

        # Store inputValueNames (it will be used in data() to return only
        # valueNames specified in config file)
        self.__inputValueNames = inputValueNames

    def data(self):
        # No simple way to test if an interator is empty
        noData = True

        for line in self.__fd:
            noData = False  # If there is a least one line, set noData to False

            try:
                jsonReader = json.loads(line)
            except Exception as e:
                log.debug('line: ' + str(line))
                log.error("Failed to parse JSON source file.")
                raise e

            values = {}
            for inputName in self.__inputValueNames:
                if inputName in jsonReader:
                    values[inputName] = jsonReader[inputName]
                else:
                    log.error(inputName+" was set in config file but "
                              "was not found in JSON input file, exiting...")
                    raise ValueNameNotFoundInJSONFile

            log.debug("JSONReader returns: " + str(values))
            yield values

        # Print a warning if file is empty
        if noData:
            log.warning("No data found (empty input file)")
            yield {}  # Return a generator with one element: {}

    def close(self):
        self.__fd.close()

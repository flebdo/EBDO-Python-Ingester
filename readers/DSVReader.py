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
import csv


# Custom DSV exceptions
class ColumnNameNotFoundInDSVFile(Exception):
    """
    A column name set in config file was not found in DSV file
    """
    pass

class ProvidedHeaderIsNotCorrect(Exception):
    """
    The header provided in config file is not correct (wrong length)
    """
    pass


class DSVReader():
    """
    This reader is iterable.

    Parameters:
        - fd: (fd) file descriptor of input file
        - inputValueNames: (tuple or list of str) Column names specified in config file
        - delimiter: (str) delimiter (e.g. ',' in CSV)
        - header: (list of str) list of value names (aka header) or None for auto-discovering
        - strictParsing: (bool) Enable strict parsing mode of csv library
    Return:
        data():
            - an iterable object,
              each iteration returns a dictionary {valueName: value, ...}
    """

    def __init__(self, fd, inputValueNames, delimiter, header, strictParsing):

        # Store fd (used by close() method)
        self.__fd = fd

        # Open csv reader
        try:
            self.__csvReader = csv.reader(fd,
                                           delimiter=delimiter,
                                           strict=strictParsing
                                           )
        except Exception as e:
            log.error("Python CSV reader raises an exception")
            print(e)
            raise e

        # Store index of columns, searchable by column name
        self.__columnsIndexes = {}

        # Get header (only used in __init__) if not given in config
        if header is None:  # Auto-discovering mode (first line is header)
            log.debug("Auto-discovering header...")
            try:
                header = next(self.__csvReader)  # List
            except StopIteration:
                log.error("No header found (empty file)")
            log.debug('Auto-discovered header: ' + str(header))

            # Find indexes of used columns
            for inputName in inputValueNames:
                if inputName in header:
                    self.__columnsIndexes[inputName] = header.index(inputName)
                else:
                    log.error(inputName + " was set in config file but was not "
                              "found in CSV file header")
                    raise ColumnNameNotFoundInDSVFile
        else:  # Used header given in config file
            # The header provided by config must be consistent with number of converters
            if len([x for x in header if x is not None]) != len(inputValueNames):
                log.error("Length of header is different from number of converters")
                raise ProvidedHeaderIsNotCorrect

            # Check if provided header contains all converters
            for inputName in inputValueNames:
                if inputName not in header:
                    log.error(inputName + " was set in config file but was not "
                              "found in the header specified in config file")
                    raise ColumnNameNotFoundInDSVFile
            # As we know the length is equal, end of previous loop means
            # that the header is consistent. We can't check the order.
            for i in range(len(header)):  # Header must be in the right order
                if header[i] is not None:
                    self.__columnsIndexes[header[i]] = i

    def data(self):
        # No simple way to test if an interator is empty
        noData = True
        for row in self.__csvReader:
            noData = False  # If there is a least one line, set noData to False
            values = {}
            for inputName in self.__columnsIndexes:
                values[inputName] = row[self.__columnsIndexes[inputName]]
            log.debug("CSVReader returns: " + str(values))
            yield values
        # Raise noData exception if self.__csvReader is empty
        if noData:
            log.warning("No data found (empty file or only header)")
            yield {}  # Return a generator with one element: {}

    def close(self):
        self.__fd.close()

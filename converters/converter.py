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

import sys
import logging as log
from datetime import datetime


# Custom exceptions
class defaultNotDefined(Exception):
    """
    Exception raised if no default value is specified and one value is empty
    """
    pass

class typeNotSupported(Exception):
    """
    Exception raised if type specified in config file is not supported
    """
    pass

class TypeMismatchForInput(Exception):
    """
    Exception raised if input type is different from input type specified
    in config file
    """
    pass

class TypeConversionFailed(Exception):
    """
    Exception raised if python is unable to convert type (e.g. float("a"))
    """
    pass

class FailedToParseDate(Exception):
    """
    Exception raised if strptime failed to parse date with format specified
    in config file
    """
    pass


class Converter():

    def __init__(self):
        pass

    def ParseType(self, string):
        """
        Parse string and return type object
        """
        if string == "str":
            return str
        elif string == "int":
            return int
        elif string == "float":
            return float
        elif string == "list":
            return list
        else:
            log.error("Type " + string + " is not supported")
            raise typeNotSupported


    def convertValue(self, value, inputType, outputType, noneValues, defaultValueDefined, defaultValue):
        """
        This function converts type of the value using standard python libraries
        and fill the value if empty with defaultValue (if exists in config)

        Parameters:
        value: (str, int, float, list, None) value to convert
        inputType: (python type object) Expected type of the input value
        outputType: (python type object) Type to convert the input value into
        noneValues: (list) List of python objects (str, None object, etc) that
                           should be considered as empty value
                           (and thus filled with defaultValue if defined)
        defaultValueDefined: (bool) Is default value defined in config file
        defaultValue: (str, int, float, list, None) value to return if value is empty,
                      None if defaultValueDefined is False

        """

        # Check input type
        if type(value) != inputType:

            log.error("Type mismatch for input.")
            log.error("value: " + str(value))
            log.error("Expected type: " + str(inputType))
            log.error("Input type: " + str(type(value)))
            raise TypeMismatchForInput

        # Sanitize string values:
        # Suppress spaces at the beginning and at the end
        if type(value) == str:
            value = value.strip()

        if value in noneValues:
            log.debug("Value is empty (according to noneValues config)")

            if defaultValueDefined:
                value = defaultValue
                log.debug("Default value found in config file: " + str(defaultValue))
            else:
                raise defaultNotDefined

        # outputType is an type object but None object is not callable
        if outputType is None or value is None:
            return None
        else:
            try:
                outputValue = outputType(value)
                return outputValue
            except Exception:
                log.exception("Failed to convert type")
                log.debug("Original value: " + str(value))
                log.debug("Trying to convert to type: " + str(outputType))
                raise TypeConversionFailed


    def convertDict(self, data, configConverters, configFormat):
        """
        Check and convert data according to configConverters and configFormat.

        Parameters:
        data is a dictionary: {inputName: value, ...}
        configConverters: {inputName: {inputName: '', outputName: '', ...}, ...}
        configFormat: {'noneValues': ['', None, etc]}

        This function is called by Ingester class and returns dictionary with checked
        and converted data that will be used by writer.
        """

        converted_data = {}
        for inputName in data.keys():
            outputName = configConverters[inputName]['outputName']

            # Handle timestamp type
            if configConverters[inputName]['outputType'] == 'timestamp':
                log.debug("Trying to parse value as date...")
                try:
                    date = datetime.strptime(data[inputName],
                                    configConverters[inputName]['dateFormat']
                                    )
                except ValueError:
                    log.error("Failed to parse \"" + data[inputName] + "\": date "\
                    "format defined in config file is not correct.")
                    raise FailedToParseDate

                if configConverters[inputName]['convertToEpoch']:
                    converted_data[outputName] = int(datetime.timestamp(date)*1000)  # epoch (in milliseconds)
                else:
                    converted_data[outputName] = data[inputName]

                continue  # no need of convertValue

            # Don't add long/lat to converted_data (will be processed after)
            if configConverters[inputName]['outputType'] in ('latitude', 'longitude'):
                continue

            # Convert value
            try:
                converted_data[outputName] = self.convertValue(data[inputName],
                    self.ParseType(configConverters[inputName]['inputType']),
                    self.ParseType(configConverters[inputName]['outputType']),
                    configFormat['noneValues'],
                    True if 'defaultValue' in configConverters[inputName] else False,
                    configConverters[inputName]['defaultValue'] if 'defaultValue'
                    in configConverters[inputName] else None
                    )
            except defaultNotDefined:
                log.error("Error during conversion.")
                log.error("Value "+inputName+" is empty and no default value was specified")
                log.error("data :"+str(data))
                raise defaultNotDefined

            log.info(inputName + ' imported to ' + outputName +
                     ' (' +
                     configConverters[inputName]['inputType'] +
                     ' --> ' +
                     configConverters[inputName]['outputType'] +
                     ')'
                     )

        # Convert lat/long data to format expected by ES
        if 'latitudeInputName' in configFormat['elasticsearch'] and \
           'longitudeInputName' in configFormat['elasticsearch']:
            try:
                converted_data['location'] = {
                                                'lat': str(data[configFormat['elasticsearch']['latitudeInputName']]).strip(),
                                                'lon': str(data[configFormat['elasticsearch']['longitudeInputName']]).strip()
                                             }
                log.debug("lat/long converted to elasticsearch geo data format: " +
                          str(converted_data['location']))
            except Exception as e:
                log.error('Failed to import location data in ElasticSearch')
                log.debug('data:' + str(data))
                raise e
        else:
            log.debug('lat/long NOT converted to elasticsearch geo data format')

        log.debug('convertedData: ' + str(converted_data))
        log.debug('---')

        return converted_data

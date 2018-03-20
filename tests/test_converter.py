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
Test converter with unittest
"""

import unittest
import logging as log
from converters.converter import *


class TestConverter(unittest.TestCase):

    def test_Converter(self):

        Testsuite = [
            {
            'description': "input type does not exist",
            'data': {"Sea_temperature": "20"},
            'configConverters': {
                'Sea_temperature': {'inputType': 'ThisTypeDoesNotExist',
                                    'inputName': 'Sea_temperature',
                                    'outputName': 'Sea_temperature',
                                    'outputType': 'float'
                                    }
                },
            'Exception': "typeNotSupported"
            },
            {
            'description': "type mismatch for input",
            'data': {"Sea_temperature": 25},
            'configConverters': {
                'Sea_temperature': {'inputType': 'str',
                                    'inputName': 'Sea_temperature',
                                    'outputName': 'Sea_temperature',
                                    'outputType': 'float'
                                    }
                },
            'Exception': "TypeMismatchForInput"
            },
            {
            'description': "type mismatch for input (2)",
            'data': {"Sea_temperatures": "[25,24,23,21,24,25]"},
            'configConverters': {
                'Sea_temperatures': {'inputType': 'list',
                                     'inputName': 'Sea_temperatures',
                                     'outputName': 'Sea_temperatures',
                                     'outputType': 'list'
                                     }
                },
            'Exception': "TypeMismatchForInput"
            },
            {
            'description': "Empty value with default value specified",
            'data': {"latitude": ""},
            'configConverters': {
                'latitude': {'inputType': 'str',
                             'inputName': 'latitude',
                             'outputName': 'Latitude',
                             'outputType': 'str',
                             'defaultValue': 'thisIsAValue'
                            }
                },
            'result': {'Latitude': "thisIsAValue"}
            },
            {
            'description': "Empty value with default value specified (custom noneValues)",
            'data': {"latitude": "N/A"},
            'configConverters': {
                'latitude': {'inputType': 'str',
                             'inputName': 'latitude',
                             'outputName': 'Latitude',
                             'outputType': 'str',
                             'defaultValue': 'thisIsAValue'
                            }
                },
            'configFormat': {'noneValues': ['', 'N/A']},
            'result': {'Latitude': "thisIsAValue"}
            },
            {
            'description': "Empty value with NO default value specified",
            'data': {"latitude": ""},
            'configConverters': {
                'latitude': {'inputType': 'str',
                             'inputName': 'latitude',
                             'outputName': 'Latitude',
                             'outputType': 'str'
                            }
                },
            'Exception': "defaultNotDefined"
            },
            {
            'description': "Date format is correct",
            'data': {"Time of Observation": "2010-08-01T00:00:00"},
            'configConverters': {
                'Time of Observation': {'inputType': 'str',
                                        'inputName': 'Time of Observation',
                                        'outputName': 'timestamp',
                                        'outputType': 'timestamp',
                                        'dateFormat': "%Y-%m-%dT%H:%M:%S",
                                        'convertToEpoch': False
                                        }
                },
            'result': {'timestamp': '2010-08-01T00:00:00'}
            },
            {
            'description': "Date format is correct (convertToEpoch is True)",
            'data': {"Time of Observation": "2010-08-01T00:00:00"},
            'configConverters': {
                'Time of Observation': {'inputType': 'str',
                                        'inputName': 'Time of Observation',
                                        'outputName': 'timestamp',
                                        'outputType': 'timestamp',
                                        'dateFormat': "%Y-%m-%dT%H:%M:%S",
                                        'convertToEpoch': True
                                        }
                },
            'result': {'timestamp': 1280613600000}
            },
            {
            'description': "Date format is incorrect",
            'data': {"Time of Observation": "2010-08-01T00:00:00"},
            'configConverters': {
                'Time of Observation': {'inputType': 'str',
                                        'inputName': 'Time of Observation',
                                        'outputName': 'timestamp',
                                        'outputType': 'timestamp',
                                        'dateFormat': "%Y---%m---%dT%H---%M---%S"
                                        }
                },
            'Exception': "FailedToParseDate"
            },
            {
            'description': "Date format is incorrect (convertToEpoch is True)",
            'data': {"Time of Observation": "2010-08-01T00:00:00"},
            'configConverters': {
                'Time of Observation': {'inputType': 'str',
                                        'inputName': 'Time of Observation',
                                        'outputName': 'timestamp',
                                        'outputType': 'timestamp',
                                        'dateFormat': "%Y---%m---%d___%H---%M---%S",
                                        'convertToEpoch': True
                                        }
                },
            'Exception': "FailedToParseDate"
            },
            {
            'description': "Empty input value and no default specified",
            'data': {"Sea_temperature": ""},
            'configConverters': {'Sea_temperature': {'inputType': 'str',
                                                      'inputName': 'Sea_temperature',
                                                      'outputName': 'Sea_temperature',
                                                      'outputType': 'float'
                                                      }
                                 },
            'Exception': "defaultNotDefined"
            },
            {
            'description': "Output type does not exist",
            'data': {"Sea_temperature": "20"},
            'configConverters': {'Sea_temperature': {'inputType': 'str',
                                                      'inputName': 'Sea_temperature',
                                                      'outputName': 'Sea_temperature',
                                                      'outputType': 'ThisTypeDoesNotExist'
                                                      }
                                 },
            'Exception': "typeNotSupported"
            },
            {
            'description': "One value (no type conversion)",
            'data': {"Latitude": "12.15"},
            'configConverters': {'Latitude': {'inputType': 'str',
                                               'inputName': 'Latitude',
                                               'outputName': 'latitude',
                                               'outputType': 'str'
                                               }
                                 },
            'configFormat': {},
            'result': {'latitude': '12.15'}
            },
            {
            'description': "Two values (no conversion)",
            'data': {"Latitude": "12.15", "Longitude": "3.25"},
            'configConverters': {
                'Latitude': {'inputType': 'str',
                             'inputName': 'Latitude',
                             'outputName': 'latitude',
                             'outputType': 'str'
                             },
                'Longitude': {'inputType': 'str',
                              'inputName': 'Longitude',
                              'outputName': 'longitude',
                              'outputType': 'str'
                              }
                },
            'result': {'latitude': '12.15', 'longitude': '3.25'}
            },
            {
            'description': "Extra spaces (at the beginning and at the end) will be removed",
            'data': {"Latitude": " 12.15 ", "Longitude": " 3.89"},
            'configConverters': {
                'Latitude': {'inputType': 'str',
                             'inputName': 'Latitude',
                             'outputName': 'latitude',
                             'outputType': 'str'
                             },
                'Longitude': {'inputType': 'str',
                              'inputName': 'Longitude',
                              'outputName': 'longitude',
                              'outputType': 'str'
                              }
                },
            'result': {'latitude': '12.15', 'longitude': '3.89'}
            },
            {
            'description': "Degrees, double quotes and simple quotes parsing",
            'data': {"Latitude": "12° 15\" 145'"},
            'configConverters': {'Latitude': {'inputType': 'str',
                                               'inputName': 'Latitude',
                                               'outputName': 'latitude',
                                               'outputType': 'str'}
                                               },
            'configFormat': {},
            'result': {'latitude': '12° 15" 145\''}
            },
            {
            'description': "Handle geo data",
            'data': {"Latitude": "47.3",
            "Longitude": "14.7"
            },
            'configConverters': {
                'Longitude': {'inputType': 'str',
                              'inputName': 'Longitude',
                              'outputName': 'longitude',
                              'outputType': 'longitude'
                              },
                'Latitude': {'inputType': 'str',
                             'inputName': 'Latitude',
                             'outputName': 'latitude',
                             'outputType': 'latitude'
                             }
                },
            'configFormat': {'elasticsearch': {'latitudeInputName': 'Latitude',
                                              'longitudeInputName': 'Longitude'}
                            },
            'result': {'location': {'lat': '47.3',
                                    'lon': '14.7'
                                    }
                      }
            },
            {
            'description': "Multiple values with type conversion",
            'data': {"Latitude": "47.3",
            "Longitude": "14.7",
            "Time of Observation": "2010-08-01T00:00:00",
            "Wind Direction": "270",
            "Wind Speed": "20"
            },
            'configConverters': {
                'Longitude': {'inputType': 'str',
                              'inputName': 'Longitude',
                              'outputName': 'longitude',
                              'outputType': 'str'
                              },
                'Latitude': {'inputType': 'str',
                             'inputName': 'Latitude',
                             'outputName': 'latitude',
                             'outputType': 'str'
                             },
                'Wind Speed': {'inputType': 'str',
                               'inputName': 'Wind Speed',
                               'outputName': 'wind_speed',
                               'outputType': 'int'
                               },
                'Time of Observation': {'inputType': 'str',
                                        'inputName': 'Time of Observation',
                                        'outputName': 'timestamp',
                                        'outputType': 'str',
                                        'dateFormat': "%Y-%m-%dT%H:%M:%S"
                                        },
                'Wind Direction': {'inputType': 'str',
                                   'inputName': 'Wind Direction',
                                   'outputName': 'wind_direction',
                                   'outputType': 'int'
                                   }
                },
            'result': {'latitude': '47.3',
                    'longitude': '14.7',
                    'timestamp': '2010-08-01T00:00:00',
                    'wind_direction': 270,
                    'wind_speed': 20
                    }
            }
            ]

        print("> Testing Converter...")

        converter = Converter()
        for testcase in Testsuite:
            print(testcase['description'])
            log.debug("data: " + str(testcase['data']))

            # Provide default values
            if 'configFormat' not in testcase:
                testcase['configFormat'] = {}
            if 'noneValues' not in testcase['configFormat']:
                testcase['configFormat']['noneValues'] = ['', None, 'N/A']
            if 'elasticsearch' not in testcase['configFormat']:
                testcase['configFormat']['elasticsearch'] = {}

            if 'result' in testcase:
                self.assertEqual(converter.convertDict(testcase['data'],
                                                       testcase['configConverters'],
                                                       testcase['configFormat']),
                                                       testcase['result']
                                                       )

            elif 'Exception' in testcase:
                exception_class = eval(testcase['Exception'])
                with self.assertRaises(exception_class):
                    converter.convertDict(testcase['data'],
                                          testcase['configConverters'],
                                          testcase['configFormat']
                                          )

            else:
                log.error("Unknown test (no 'result' or 'Exception' section found)")

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
Test readers with unittest
"""

import unittest
import io
import logging as log

from readers.DSVReader import *
from readers.JSONReader import *

class TestReaders(unittest.TestCase):

    def test_DSVReader(self):

        Testsuite = [
            {
            'description': "Empty line",
            'line': '',
            'inputValueNames': [],
            'delimiter': ',',
            'header': None,
            'result': {}
            },
            {
            'description': "Only spaces",
            'line': '        ',
            'inputValueNames': [],
            'delimiter': ',',
            'header': None,
            'result': {}
            },
            {
            'description': "Only one line (auto-discovering header)",
            'line': 'HeaderOnly\n',
            'inputValueNames': [],
            'delimiter': ',',
            'header': None,
            'result': {}  # header only => no data
            },
            {
            'description': "Only one line (header defined in conf)",
            'line': 'value\n',
            'inputValueNames': ['testname'],
            'delimiter': ',',
            'header': ['testname'],
            'result': {'testname': 'value'}
            },
            {
            'description': "Header defined in config not consistent (column is missing)",
            'line': 'latitudeValue,longitudeValue',
            'inputValueNames': ['Latitude','Longitude'],
            'delimiter': ',',
            'header': ['Latitude'],
            'Exception': "ProvidedHeaderIsNotCorrect"
            },
            {
            'description': "Header defined in config not consistent (converter is missing)",
            'line': 'latitudeValue,longitudeValue',
            'inputValueNames': ['Latitude'],
            'delimiter': ',',
            'header': ['Latitude','Longitude'],
            'Exception': "ProvidedHeaderIsNotCorrect"
            },
            {
            'description': "Column name in config file but not in DSV file",
            'line': 'Latitude\n13.37',
            'inputValueNames': ['Time'],
            'delimiter': ',',
            'header': None,
            'Exception': "ColumnNameNotFoundInDSVFile"
            },
            {
            'description': "One column and one line of data",
            'line': 'Latitude\n12.15',
            'inputValueNames': ['Latitude'],
            'delimiter': ',',
            'header': None,
            'result': {'Latitude': '12.15'}
            },
            {
            'description': "One line of data (header defined in config)",
            'line': '12.15',
            'inputValueNames': ['Latitude'],
            'delimiter': ',',
            'header': ['Latitude'],
            'result': {'Latitude': '12.15'}
            },
            {
            'description': "2 columns (separated by ',') and one line of data",
            'line': 'Latitude,Longitude\n12.15,3.89',
            'inputValueNames': ['Latitude','Longitude'],
            'delimiter': ',',
            'header': None,
            'result': {'Latitude': '12.15', 'Longitude': '3.89'}
            },
            {
            'description': "2 columns (separated by '|') and one line of data",
            'line': 'Latitude|Longitude\n12.15|3.89',
            'inputValueNames': ['Latitude','Longitude'],
            'delimiter': '|',
            'header': None,
            'result': {'Latitude': '12.15', 'Longitude': '3.89'}
            },
            {
            'description': "Spaces should NOT be removed by reader",
            'line': 'Latitude,Longitude\n 12.15 , 3.89 ',
            'inputValueNames': ['Latitude','Longitude'],
            'delimiter': ',',
            'header': None,
            'result': {'Latitude': ' 12.15 ', 'Longitude': ' 3.89 '}
            },
            {
            'description': "Parsing degrees, double quotes and simple quotes",
            'line': 'Latitude\n12° 15" 145\'',
            'inputValueNames': ['Latitude'],
            'delimiter': ',',
            'header': None,
            'result': {'Latitude': '12° 15" 145\''}
            },
            {
            'description': "5 attributes and one line of data",
            'line': 'Latitude,Longitude,Time of Observation,Wind Direction,'\
                    'Wind Speed\n47.3,14.7,2010-08-01T00:00:00,270,20',
            'inputValueNames': ['Longitude','Latitude','Wind Speed',
                                'Time of Observation','Wind Direction'],
            'delimiter': ',',
            'header': None,
            'result': {'Latitude': '47.3',
                    'Longitude': '14.7',
                    'Time of Observation': '2010-08-01T00:00:00',
                    'Wind Direction': '270',
                    'Wind Speed': '20'
                    }
            }
            ]

        print("> Testing DSVReader...")
        for testcase in Testsuite:
            print(testcase['description'])
            log.debug("line: " + testcase['line'])

            if 'result' in testcase:
                source = DSVReader(io.StringIO(testcase['line']),
                                   testcase['inputValueNames'],
                                   testcase['delimiter'],
                                   testcase['header'],
                                   strictParsing if 'strictParsing' in testcase else True
                                   )
                # Use a loop because else StopIteration raised by
                # source.data() will kill loop on Testsuite
                for d in source.data():
                    result = d # source.data() has one element
                source.close()
                self.assertEqual(result, testcase['result'])

            elif 'Exception' in testcase:
                exception_class = eval(testcase['Exception'])
                with self.assertRaises(exception_class):
                    source = DSVReader(io.StringIO(testcase['line']),
                                       testcase['inputValueNames'],
                                       testcase['delimiter'],
                                       testcase['header'],
                                       strictParsing if 'strictParsing' in testcase else True
                                       )
                    # Loop on data because some exceptions could be raised
                    # only if a call of data() was made
                    for d in source.data():
                        pass

            else:
                log.error("Unknown test \
                          (no 'result' or 'Exception' section found)")


    def test_JSONReader(self):

        Testsuite = [
            {
            'description': "Empty file",
            'line': '',
            'inputValueNames': [],
            'result': {}
            },
            {
            'description': "Value name in config file but not in JSON file",
            'line': '{"Latitude": "47.3"}',
            'inputValueNames': ['Time'],
            'Exception': "ValueNameNotFoundInJSONFile"
            },
            {
            'description': "Simple couple of {'value name': 'value'}",
            'line': '{"Latitude": "47.3"}',
            'inputValueNames': ['Latitude'],
            'result': {'Latitude': '47.3'}
            },
            {
            'description': "2 couples of {'value name': 'value'}",
            'line': '{"Latitude": "47.3", "Longitude": "3.89"}',
            'inputValueNames': ['Latitude','Longitude'],
            'result': {'Latitude': '47.3', 'Longitude': '3.89'}
            },
            {
            'description': "Spaces should not be removed by reader",
            'line': '{"Latitude": " 47 .3", "Longitude": " 3.89 "}',
            'inputValueNames': ['Latitude','Longitude'],
            'result': {'Latitude': ' 47 .3',
                       'Longitude': ' 3.89 '}
            },
            {
            'description': "Parsing degrees, double quotes and simple quotes",
            'line': '{"Latitude": "12° 15\\" 145\'"}',
            'inputValueNames': ['Latitude'],
            'result': {'Latitude': '12° 15" 145\''}
            },
            {
            'description': "Value is a list represented by a string (no conversion should be done)",
            'line': '{"timestamp":"2009-11-15T14:12:20.000+01:00", "TOB": "[1,2,3]"}',
            'inputValueNames': ['timestamp','TOB'],
            'result': {
                'timestamp': '2009-11-15T14:12:20.000+01:00',
                'TOB': "[1,2,3]"  # Reader is not supposed to do any type conversion
                }
            },
            {
            'description': "Value is a list",
            'line': '{"TOB": [1,2,3]}',
            'inputValueNames': ['TOB'],
            'result': {
                'TOB': [1,2,3]  # JSON is a typed format
                }
            },
            {
            'description': "Value is a float",
            'line': '{"value": 45.5216578}',
            'inputValueNames': ['value'],
            'result': {
                'value': 45.5216578
                }
            }
            ]

        print("> Testing JSONReader...")
        for testcase in Testsuite:
            print(testcase['description'])
            log.debug("line: " + testcase['line'])

            if 'result' in testcase:
                source = JSONReader(io.StringIO(testcase['line']),
                                    testcase['inputValueNames']
                                   )
                # Use a loop because else StopIteration raised by
                # source.data() will kill loop on Testsuite
                for d in source.data():
                    result = d  # source.data() has one element
                source.close()
                self.assertEqual(result, testcase['result'])

            elif 'Exception' in testcase:
                exception_class = eval(testcase['Exception'])
                with self.assertRaises(exception_class):
                    source = JSONReader(io.StringIO(testcase['line']),
                                        testcase['inputValueNames']
                                       )
                    # Loop on data because some exceptions could be raised
                    # only if a call of data() was made
                    for d in source.data():
                        pass

            else:
                log.error("Unknown test (no 'result' or 'Exception' section found)")

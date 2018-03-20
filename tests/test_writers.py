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
Test writers with unittest
"""

import unittest
import io
import logging as log
import json
from writers.JSONWriter import *


class TestWriters(unittest.TestCase):

        def test_JSONWriter(self):

            Testsuite = [
                {
                'description': "No data",
                'data': {},
                },
                {
                'description': "One value (str)",
                'data': {"Latitude": "47.3"},
                },
                {
                'description': "Two values (str)",
                'data': {"Latitude": "47.3",
                         "Longitude": "47.3"},
                },
                {
                'description': "One value (float)",
                'data': {"Temp": 45.856},
                },
                {
                'description': "Empty value",
                'data': {"Latitude": ""},
                },
                {
                'description': "Empty value name",
                'data': {"": "5"},
                }
                ]

            print("> Testing JSONWriter...")
            for testcase in Testsuite:
                print(testcase['description'])
                log.debug("data: ", testcase['data'])

                fd = io.StringIO()
                destination = JSONWriter(fd)
                destination.write(testcase['data'])

                """json.dump method use multiple calls to open.write(),
                it's difficult to assert an exact match.
                Using poor's man test: only test if data (key and value)
                has been written"""

                writtenData = fd.getvalue()
                for key in testcase['data']:
                    self.assertTrue(str(key) in writtenData)
                    self.assertTrue(str(testcase['data'][key]) in writtenData)

                # Try to load data and compare it
                try:
                    loadedData = json.loads(writtenData)
                except:
                    self.fail('json module failed to load written data')
                self.assertEqual(loadedData, testcase['data'])

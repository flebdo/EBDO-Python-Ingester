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
Test schemes with unittest
"""

import unittest
import unittest.mock as mock
import io
import logging as log

from schemes.local import *


class TestSchemes(unittest.TestCase):

        def test_local(self):

            Testsuite = [
                {
                'description': "0-size string",
                'data': '',
                },
                {
                'description': "random string",
                'data': 'Fje8QBthKqdVjSjfCCORd0U44lQmIqP1zuISQN4q8W7zFnnjxa',
                },
                {
                'description': "random string with special characters",
                'data': 'bd@) !&*(çé_"ù ',
                },
                {
                'description': "unicode characters",
                'data': '×ØÙÚÝÞß0E'
                }
                ]

            print("> Testing local (reading)...")
            for testcase in Testsuite:
                print(testcase['description'])
                log.debug("data: ", testcase['data'])

                with mock.patch('schemes.local.open',
                                return_value=io.StringIO(testcase['data'])
                               ):
                    source = LocalFile(None, 'read').fd

                result = source.read()
                self.assertEqual(result, testcase['data'])

            print("> Testing local (writing)...")
            for testcase in Testsuite:
                print(testcase['description'])
                log.debug("data: ", testcase['data'])

                m = mock.mock_open()
                with mock.patch('schemes.local.open', m):
                    destination = LocalFile(None, 'write').fd

                destination.write(testcase['data'])
                self.assertTrue(mock.call(testcase['data']) in m.mock_calls)

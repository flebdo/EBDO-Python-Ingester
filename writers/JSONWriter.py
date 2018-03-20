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
JSON writer

Parameter:
    - fd: (fd) file descriptor of output file

Methods:
    - write:
        - data: (dict) data to write as json
    - close: close fd of output file
"""

import logging as log
import json


class JSONWriter():

    def __init__(self, fd):
        self.__fd = fd

    def write(self, data):
        # data is a dictionary
        json.dump(data, self.__fd, indent=4)
        self.__fd.write("\n")  # nice file format

    def close(self):
        self.__fd.close()

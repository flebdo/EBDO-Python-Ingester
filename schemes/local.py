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
Local filesystem scheme

Parameters:
filepath: (str) path to file
mode: (str) 'read' or 'write', open file in reading or writing mode
            Note that file is always opened as text (not binary)

Variable:
fd: file descriptor of opened file
"""

import logging as log

class LocalFile():
    def __init__(self, filepath, mode):

        if mode == 'read':
            # newline='' returns line endings untranslated (required for DSVReader)
            self.fd = open(filepath, 'rt', newline='')

        elif mode == 'write':
            self.fd = open(filepath, 'wt')

        else:
            log.error("Unknown mode '" + mode + "' for local scheme.")
            raise Exception('UnknownModeForLocalScheme')

#!/usr/bin/env python3

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

"""Ingester

Usage:
  ingester.py [-v | -vv] [--progress] (-c | --config) <config_paths>...
  ingester.py (-h | --help)
  ingester.py (-V |--version)

Options:
  -c --config    Paths to config files (separated by spaces or wildcard)
  -v -vv         Increase verbosity level to INFO or DEBUG. Default to WARNING.
  -p --progress  Show progress
  -h --help      Show this screen
  -V --version   Show version

Return codes:
0 : successful conversion
1 : error during conversion
"""

version = "0.1"


# import required modules
import sys, os
from docopt import docopt
import logging as log
import yaml

# Only required reader and writer will be imported
from converters.converter import Converter


class Ingester():
    """
    Ingester class
    Parameters:
        configPath: (str) path to YAML config file
        logLevel: (int) verbosity (according to logging module values)
        showProgress: (bool) Show progress of ingestion
    """

    def __init__(self, configPath, logLevel, showProgress):

        # Setup log format
        log.basicConfig(format='%(levelname)s:%(message)s', level=logLevel)

        # Ingestion
        self.ingest(configPath, showProgress)


    def parseConfig(self, configPath):
        """
        Parse YAML config file and return a hierarchically-organized dictionary
        """
        # Open and parse config file
        # Warning: It is not safe to call yaml.load with any data received from
        # an untrusted source!
        try:
            configFile = open(configPath, 'rt')
            config = yaml.load(configFile)
            configFile.close()
        except Exception as e:
            log.exception("Can't parse YAML file.")
            raise e
        log.debug('config: ' + str(config))

        # Store I/O config
        self.__configInput = config['input']
        self.__configOutput = config['output']
        self.__configFormat = config['format']
        self.__configFormat['elasticsearch'] = {}

        # Store config of converters in a nice format (searchable by inputName...)
        # and check if config is consistent
        self.__configConverters = {}
        for definition in config['converters']:
            self.__configConverters[definition['inputName']] = definition

            # Check consistency of config
            if definition['outputType'] == 'timestamp' and \
                self.__configInput['scheme'] == 'elasticsearch':
                    if 'dateFormat' not in definition or not definition['sanitizeDate']:
                        raise KeyError("If elasticsearch scheme is used "
                        "dateFormat must be specified and sanitizeDate "
                        "set to True")
            if definition['outputType'] == 'latitude':
                self.__configFormat['elasticsearch']['latitudeInputName'] = definition['inputName']
            if definition['outputType'] == 'longitude':
                self.__configFormat['elasticsearch']['longitudeInputName'] = definition['inputName']

        if ('latitudeInputName' in self.__configFormat['elasticsearch']) ^ \
        ('longitudeInputName' in self.__configFormat['elasticsearch']):  # XOR
            log.warning("Only one block has outputType set to latitude or longtitude. "
                        "It's maybe not what you want.")


    def initializeSource(self):
        # Parse input and open fd
        if self.__configInput['scheme'] == 'local':
            from schemes import local

            try:
                inputFd = local.LocalFile(self.__configInput['local']['path'], 'read').fd
            except Exception as e:
                log.error("Failed to open input file.")
                raise e

        elif self.__configInput['scheme'] == 'hdfs':
            raise NotImplementedError("HDFS scheme not yet implemented")

        else:
            raise NotImplementedError("Unknown input scheme: " + self.__configInput['scheme'])

        # Open reader
        filetype = self.__configInput['format']['type'].upper()
        if filetype == 'DSV':
            from readers.DSVReader import DSVReader

            try:  # Use try/except to avoid nested if
                dsvConfig = self.__configInput['format']['dsv']
            except KeyError:
                raise KeyError("DSV format not configured in config file")

            for param in ('delimiter', 'strictParsing'):
                if param not in dsvConfig:
                    raise KeyError("DSV '" + param + "' not configured in config file")

            try:
                self.__source = DSVReader(inputFd,
                                          tuple(self.__configConverters.keys()),
                                          dsvConfig['delimiter'],
                                          dsvConfig['header'] if 'header' in dsvConfig else None,
                                          dsvConfig['strictParsing']
                                          )
            except Exception as e:
                log.exception("DSV Reader failed")
                raise e

        elif filetype == 'JSON':
            from readers.JSONReader import JSONReader

            try:
                self.__source = JSONReader(inputFd, tuple(self.__configConverters.keys()))
            except Exception as e:
                log.exception("JSON Reader failed")
                raise e

        else:
            raise NotImplementedError("Unknown input type: " + filetype)


    def initializeDestination(self):
        if self.__configOutput['scheme'] == 'elasticsearch':
            from writers.ESWriter import ESWriter

            es_config = self.__configOutput['elasticsearch']
            self.__destination = ESWriter(host=es_config['host'],
                                          port=es_config['port'],
                                          index=es_config['index']
                                          )

        else:
            # Open fd
            if self.__configOutput['scheme'] == 'local':
                from schemes import local  # Required if input scheme is not local

                try:
                    outputFd = local.LocalFile(self.__configOutput['local']['path'], 'write').fd
                except Exception as e:
                    log.error("Failed to open output file.")
                    raise e

            elif self.__configOutput['scheme'] == 'hdfs':
                raise NotImplementedError("HDFS scheme not yet implemented")

            else:
                raise NotImplementedError("Unknown output scheme: " + self.__configOutput['scheme'])

            # Open writer
            from writers.JSONWriter import JSONWriter
            try:
                self.__destination = JSONWriter(outputFd)
            except Exception as e:
                log.exception("Failed to open JSON writer file.")
                raise e


    def convertValues(self, showProgress):
        # Loop on data
        nbRowProcessed = 0
        for data in self.__source.data():
            # data is a dictionary: {inputName: value, ...}

            # Check, convert and write data
            self.__destination.write(self.__converter.convertDict(data,
                                                                  self.__configConverters,
                                                                  self.__configFormat
                                                                 )
                                    )

            # Print progress
            if showProgress:
                print(nbRowProcessed, end='\r')
            nbRowProcessed += 1

        if showProgress:
            print("Done: " + str(nbRowProcessed) + " lines processed with success.")


    def close(self):
        self.__source.close()
        self.__destination.close()


    def ingest(self, configPath, showProgress):
        # Parse config
        self.parseConfig(configPath)

        # Open I/O
        self.initializeSource()
        self.initializeDestination()

        # Create an instance of converter
        self.__converter = Converter()

        # Convert values
        self.convertValues(showProgress)

        # Exiting properly
        self.close()


if __name__ == '__main__':

    # Parse arguments using docopt
    arguments = docopt(__doc__, version=version)

    showProgress = arguments['--progress']
    if arguments['-v'] == 0:
        logLevel = log.WARNING
    elif arguments['-v'] == 1:
        logLevel = log.INFO
    elif arguments['-v'] >= 2:
        logLevel = log.DEBUG

    # Loop on config files given in arguments
    indexProcessedFiles = 0
    nbConfigFiles = len(arguments['<config_paths>'])

    for configPath in arguments['<config_paths>']:
        indexProcessedFiles += 1
        if showProgress or logLevel <= log.INFO:  # -v or -vv
            print("["+str(indexProcessedFiles)+"/"+str(nbConfigFiles)+"] "
                  "Processing config file " + configPath + "...")

        Ingester(configPath, logLevel, showProgress)

    log.info(str(nbConfigFiles) + " config files processed with success.")
    sys.exit(0)

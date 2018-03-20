# EBDO Ingester

Ingester is a Python-written tool helping reading, converting then writing data for the EBDO project. It can read CSV or JSON from local files or HDFS (to come), convert between types, and write JSON to local files, HDFS (to come), or ElasticSearch.
Specifications for an Ingestion-run are provided as a YAML file (see [Config file section](#config-file)).


## Requirements

Make sure you have Python 3.x installed.

Optional: elasticsearch module is required to import data directly into Elasticsearch.
```sh
pip3 install --user elasticsearch
```

## Usage

```sh
$ ./ingester.py --help
Ingester

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
```

## Config file

Ingester uses a config file in YAML format. See [examples/](examples/) for commented examples.

Config file is divided in 3 sections:
- input: define input scheme and reader specifications
- output: define output scheme
- converter: define conversion rules (input and output formats, default values)

#### Available schemes

Schemes can be "local" or "hdfs" for input/output.

- local: local filesystem
  - path: path to file (e.g. examples/data/WeatherBuoy_NOAA.csv)
- hdfs: HDFS backend
  - ip: hostname or ip address of HDFS cluster
  - port: port of HDFS cluster
  - path: path of file in HDFS filesytem

An "elasticsearch" scheme is provided for output to import data directly into elasticsearch (aka ES).
- elasticsearch:
  - host: hostname or ip address of ES instance
  - port: port of ES API
  - index: elasticsearch index where data will be imported

#### Reader specification

Ingester can handle DSV (Delimiter-separated values) and JSON file formats as input.

Configuration options depend of the input file format:
- DSV:
  - delimiter: (str) set DSV delimiter of input file (e.g. ',' or '|')
  - header: (list of str) Give header of file. null can be used to ignore a column.
  If not present the first line is assumed to be the header.
  (e.g. ['Latitude','Longitude'])
  - strictParsing: (bool) Raise an exception in case of malformed file
  - newline detection is automatically handled by DSV reader
- JSON: no configuration option are available.
        Note that JSON reader expects one valid json per line.

#### Converter configuration

This section defines relations between input and output formats.

The format section defines configuration options for all converters.  
Available options:
- noneValues: (list) Define values that should be consider as empty value (e.g. ['', null, 'N/A']). Note that null is only consistent in case of a JSON input and in this case represents the JSON null object.

Ingester can check and convert input data.
These options must be specified directly in each converter block.  
Available options:

- date: outputType of the block must be set to 'timestamp'
    - dateFormat: (str) strftime format of expected date in input
    - convertToEpoch: (bool) Convert input date to epoch (in milliseconds)
- location: used to import latitude and longitude as geo_point type in ES: outputType must be set to 'latitude' or 'longitude' in the correct blocks.

Important: dateFormat must be defined and convertToEpoch must be set to True to import data as timestamp into ES.

For each value name (equivalent to column name in DSV file), a block must be defined:
- inputName: Name of the value in the input file
- outputName: Name of the value in the output file
- inputType: Type of the value in input file
- outputType: Type of the value in output file
- defaultValue: (optional) Value to consider if input value is empty (as defined in noneValues).
Type of value must match with outputType. Note that null can be used for any outputType.

Note: type can be str, int, float, list or (for output only) a special type (e.g. timestamp). Note that DSV format is not typed (instead of JSON), thus inputType for a DSV file is always str.

Rules:
- If an inputName defined in converters does not exist in input file, an exception will be raised.
- Input and output types must be defined, even if the type is the same (explicit declaration)
- If defaultValue is set, an empty source value (as defined in noneValues) will be filled by the default value. defaultValue will be converted to outputType if needed.
- If defaultValue is NOT set (i.e. not present), an empty source value will raise an exception.

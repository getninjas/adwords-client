import logging
import csv
import tempfile
import gzip
from collections import OrderedDict


logger = logging.getLogger(__name__)


def gunzip(compressed_stream):
    with tempfile.NamedTemporaryFile(mode='wb+') as file:
        for line in compressed_stream:
            file.write(line)
        file_name = file.name
        file.flush()
        decompressed_file = gzip.open(file_name, 'rt')
    return decompressed_file


def csv_reader(data_stream, fields, converter=None):
    if converter:
        converter = [converter.get(field, lambda x: x) for field in fields]

    def fields_iterator():
        for line in csv.reader(data_stream):
            yield OrderedDict(zip(fields, map(lambda x, y: x(y), converter, line)))

    return fields_iterator

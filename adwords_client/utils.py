import logging
import csv
import tempfile
import gzip
from collections import OrderedDict

logger = logging.getLogger(__name__)


def save_report_in_disk(compressed_stream, fields, converter=None):
    if converter:
        converter = [converter.get(field, lambda x: x) for field in fields]
    with tempfile.NamedTemporaryFile(mode='wb+', delete=False) as file:
        decompressed_file = gunzip(compressed_stream)
        for line in decompressed_file:
            file.write(','.join([str(converter[idx](i)) for idx, i in enumerate(line.split(',')[:-1])]).encode())
            # compressed_file.write((','.join(list(map(lambda x, y: str(x(y)), converter, line)))+'\n').encode())
        file_name = file.name
        file.close()
    return file_name


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

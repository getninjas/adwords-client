import logging
import csv
import tempfile
import gzip
from collections import OrderedDict

logger = logging.getLogger(__name__)


def save_report_in_disk(compressed_stream, fields, converter=None):
    if converter:
        converter = [converter.get(field, lambda x: x) for field in fields]
    stream_compressed_file = gzip.GzipFile(mode='rb', fileobj=compressed_stream)
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as file:
        with gzip.GzipFile(mode='wb', fileobj=file) as result_compressed_file:
            for line in stream_compressed_file:
                decoded_line = line.decode()
                result_compressed_file.write(','.join([str(converter[idx](i)) for idx, i in enumerate(decoded_line[:-1].split(','))]+['\n']).encode())
            stream_compressed_file.close()
            return result_compressed_file.name


def gunzip(compressed_stream):
    with tempfile.NamedTemporaryFile(mode='wb+') as file:
        for line in compressed_stream:
            file.write(line)
        file_name = file.name
        file.flush()
        decompressed_file = gzip.open(56, 'rt')
    return decompressed_file


def csv_reader(data_stream, fields, converter=None):
    if converter:
        converter = [converter.get(field, lambda x: x) for field in fields]

    def fields_iterator():
        for line in csv.reader(data_stream):
            yield OrderedDict(zip(fields, map(lambda x, y: x(y), converter, line)))

    return fields_iterator

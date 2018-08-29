import logging
import tempfile
import os

logger = logging.getLogger(__name__)


class FilesystemStorage:
    """
    Just a Django-less storage w/ partial Django Storage API implemented
    """
    def __init__(self, workdir):
        self.workdir = workdir

    def open(self, name, mode='rb', *args, **kwargs):
        if name.startswith('/'):
            raise ValueError('File name should not start with "/": {}'.format(name))
        full_name = os.path.join(self.workdir, name)
        os.makedirs(os.path.dirname(full_name), exist_ok=True)
        return open(full_name, mode=mode, *args, **kwargs)

    def listdir(self, path):
        dirnames, filenames = [], []
        for _, dirnames, filenames in os.walk(os.path.join(self.workdir, path)):
            break
        return dirnames, filenames


class TemporaryFilesystemStorage(FilesystemStorage):
    """
    Just a Django-less storage w/ partial Django Storage API implemented
    """
    def __init__(self):
        self._workdir = None

    @property
    def workdir(self):
        if not self._workdir:
            self._workdir = tempfile.TemporaryDirectory()
        return self._workdir.name

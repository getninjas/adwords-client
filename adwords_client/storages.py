import tempfile
from os import path


class TemporaryFilesystemStorage:
    """
    Just a Django-less storage w/ partial Django Storage API implemented
    """
    def __init__(self):
        self._files = {}

    def open(self, name, mode='w+', *args, **kwargs):
        if name not in self._files:
            self._files[name] = tempfile.NamedTemporaryFile()
        return open(self._files[name].name, mode=mode, *args, **kwargs)


class FilesystemStorage:
    """
    Just a Django-less storage w/ partial Django Storage API implemented
    """
    def __init__(self, workdir):
        self.workdir = workdir

    def open(self, name, mode='w+', *args, **kwargs):
        return open(path.join(self.workdir, name), mode=mode, *args, **kwargs)

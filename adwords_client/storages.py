import tempfile
import os


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

    def listdir(self, path):
        folders = []
        files = []
        for key in self._files.keys():
            relative_path = key[len(path):]
            if key.startswith(path) and relative_path:
                folder_or_file, _, file_in_subfolder_or_empty = relative_path.partition('/')
                # if there is a file in a subfolder, we have found a folder to add
                # otherwise, we found a file in the current path
                if file_in_subfolder_or_empty:
                    # this folder exists in this "virtual" structure
                    folders.append(folder_or_file)
                else:
                    # the file is
                    files.append(folder_or_file)
        return folders, files


class FilesystemStorage:
    """
    Just a Django-less storage w/ partial Django Storage API implemented
    """
    def __init__(self, workdir):
        self.workdir = workdir

    def open(self, name, mode='w+', *args, **kwargs):
        return open(os.path.join(self.workdir, name), mode=mode, *args, **kwargs)

    def listdir(self, path):
        for _, dirnames, filenames in os.walk(os.path.join(self.workdir, path)):
            break
        return dirnames, filenames

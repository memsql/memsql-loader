import fcntl
import os
import tempfile
from stat import S_IRWXG, S_IRWXU, S_IRWXO
from contextlib import contextmanager

class FIFO(object):
    def __init__(self, gzip=False):
        self._closed = False
        self._reader_abort_fn = None
        self._tempdir = tempfile.mkdtemp(dir='/tmp')
        os.chmod(self._tempdir, S_IRWXU | S_IRWXG | S_IRWXO)

        self.path = os.path.join(self._tempdir, 'fifo')
        if gzip:
            self.path += '.gz'

        try:
            os.mkfifo(self.path)
            os.chmod(self.path, S_IRWXU | S_IRWXG | S_IRWXO)
        except OSError:
            os.rmdir(self._tempdir)
            raise

    def __del__(self):
        self.cleanup()

    @contextmanager
    def open(self, blocking=False):
        """ This returns a file descriptor, not a file object. The
        file descriptor must be written to with os.write()

        :param blocking: If True, the FIFO will block on write
        """

        assert not self._closed, 'FIFO has already been cleaned up'
        # Will hang until a reader attaches to the other end of the pipe,
        # which is what we want since we don't want to start writing to
        # the pipe until a reader attaches in case we finish writing
        # and close the file before the reader attaches, which would
        # drop all the data.
        with open(self.path, 'wb') as fifofile:
            try:
                fcntl.fcntl(fifofile.fileno(), fcntl.F_SETFL, 0 if blocking else os.O_NONBLOCK)
                yield fifofile
            except:
                self.abort_reader()
                raise

    def attach_reader(self, abort_function):
        assert self._reader_abort_fn is None, 'A reader is already attached to this FIFO'
        self._reader_abort_fn = abort_function

    def detach_reader(self):
        # Opening the pipe for writing above will hang until a reader attaches,
        # so we make sure that on detaching the reader, we always attempt to
        # read from the pipe to unblock it.
        try:
            os.close(os.open(self.path, os.O_RDONLY | os.O_NONBLOCK))
        except OSError:
            pass
        self._reader_abort_fn = None

    def abort_reader(self):
        if self._reader_abort_fn is not None:
            self._reader_abort_fn()
            self._reader_abort_fn = None

    def cleanup(self):
        if self._closed:
            return

        self._closed = True
        os.unlink(self.path)
        os.rmdir(self._tempdir)

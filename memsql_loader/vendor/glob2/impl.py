"""Filename globbing utility."""

from __future__ import absolute_import

import sys
import os
import re
from . import fnmatch

# MemSQL imports
import boto
import pywebhdfs.errors

class Globber(object):
    curdir = os.curdir
    fs_encoding = sys.getfilesystemencoding() or sys.getdefaultencoding()

    def _normalize_unicode(self, s):
        if not isinstance(s, unicode):
            temp = s  # this is to help with debugging
            try:
                s = unicode(temp, self.fs_encoding)
            except UnicodeDecodeError:
                # Not all filesystems support encoding input that
                # we throw. We assume UTF-8 is a sufficiently powerful
                # "catch-all" to decode things that don't match.
                s = unicode(temp, 'utf-8')
        return s

    def _normalize_string(self, s):
        if isinstance(s, unicode):
            temp = s  # this is to help with debugging
            try:
                s = temp.encode(self.fs_encoding)
            except UnicodeEncodeError:
                # Not all filesystems support encoding input that
                # we throw. We assume UTF-8 is a sufficiently powerful
                # "catch-all" to encode things that don't match.
                s = temp.encode('utf-8')
        return s

    def listdir(self, dirname, prefix=None):
        """The base listdir doesn't take advantage of the
        prefix optimization. This exists to help with S3 workloads.

        We're also making all output unicode. I confirmed that isdir, islink,
        and exists all work with unicode input."""
        return [self._normalize_unicode(x) for x in os.listdir(self._normalize_string(dirname))]

    def exists(self, f):
        return os.path.lexists(self._normalize_string(f))

    def isdir(self, f):
        return os.path.isdir(self._normalize_string(f))

    def islink(self, f):
        return os.path.islink(self._normalize_string(f))

    def walk(self, top, followlinks=False):
        """A simplified version of os.walk (code copied) that uses
        ``self.listdir``, and the other local filesystem methods.

        Because we don't care about file/directory distinctions, only
        a single list is returned.
        """
        try:
            names = self.listdir(top)
        except os.error as err:
            return

        items = []
        for name in names:
            items.append(name)

        yield top, items

        for name in items:
            new_path = os.path.join(top, name)
            if followlinks or not self.islink(new_path):
                for x in self.walk(new_path, followlinks):
                    yield x

    def glob(self, pathname, with_matches=False):
        """Return a list of paths matching a pathname pattern.

        The pattern may contain simple shell-style wildcards a la
        fnmatch. However, unlike fnmatch, filenames starting with a
        dot are special cases that are not matched by '*' and '?'
        patterns.

        """
        return list(self.iglob(pathname, with_matches))

    def iglob(self, pathname, with_matches=False):
        """Return an iterator which yields the paths matching a pathname
        pattern.

        The pattern may contain simple shell-style wildcards a la
        fnmatch. However, unlike fnmatch, filenames starting with a
        dot are special cases that are not matched by '*' and '?'
        patterns.

        If ``with_matches`` is True, then for each matching path
        a 2-tuple will be returned; the second element if the tuple
        will be a list of the parts of the path that matched the individual
        wildcards.
        """
        result = self._iglob(pathname)
        if with_matches:
            return result
        return map(lambda s: s[0], result)

    def _iglob(self, pathname, rootcall=True):
        """Internal implementation that backs :meth:`iglob`.

        ``rootcall`` is required to differentiate between the user's call to
        iglob(), and subsequent recursive calls, for the purposes of resolving
        certain special cases of ** wildcards. Specifically, "**" is supposed
        to include the current directory for purposes of globbing, but the
        directory itself should never be returned. So if ** is the lastmost
        part of the ``pathname`` given the user to the root call, we want to
        ignore the current directory. For this, we need to know which the root
        call is.
        """

        # Short-circuit if no glob magic
        if not has_magic(pathname):
            if self.exists(pathname):
                yield pathname, ()
            return

        # If no directory part is left, assume the working directory
        dirname, basename = os.path.split(pathname)

        # If the directory is globbed, recurse to resolve.
        # If at this point there is no directory part left, we simply
        # continue with dirname="", which will search the current dir.
        # `os.path.split()` returns the argument itself as a dirname if it is a
        # drive or UNC path.  Prevent an infinite recursion if a drive or UNC path
        # contains magic characters (i.e. r'\\?\C:').
        if dirname != pathname and has_magic(dirname):
            # Note that this may return files, which will be ignored
            # later when we try to use them as directories.
            # Prefiltering them here would only require more IO ops.
            dirs = self._iglob(dirname, rootcall=False)
        else:
            dirs = [(dirname, ())]

        # Resolve ``basename`` expr for every directory found
        for dirname, dir_groups in dirs:
            for name, groups in self.resolve_pattern(
                    dirname, basename, not rootcall):
                yield os.path.join(dirname, name), dir_groups + groups

    def chop_dirname(self, dirname, path):
        """This is a more generalized form -> basically if
        curdir == '', then path[len(dirname)+1] chops off the
        first valid character of the string. This function assumes
        of course that path starts with dirname."""

        if dirname:
            return path[len(dirname)+1:]
        else:
            return path

    def resolve_pattern(self, dirname, pattern, globstar_with_root):
        """Apply ``pattern`` (contains no path elements) to the
        literal directory`` in dirname``.

        If pattern=='', this will filter for directories. This is
        a special case that happens when the user's glob expression ends
        with a slash (in which case we only want directories). It simpler
        and faster to filter here than in :meth:`_iglob`.
        """

        if sys.version_info[0] == 3:
            if isinstance(pattern, bytes):
                dirname = bytes(self.curdir, 'ASCII')
        else:
            if isinstance(pattern, unicode) and not isinstance(dirname, unicode):
                dirname = unicode(dirname, sys.getfilesystemencoding() or
                                           sys.getdefaultencoding())

        # If no magic, short-circuit, only check for existence
        if not has_magic(pattern):
            if pattern == '':
                if self.isdir(dirname):
                    return [(pattern, ())]
            else:
                if self.exists(os.path.join(dirname, pattern)):
                    return [(pattern, ())]
            return []

        if not dirname:
            dirname = self.curdir

        try:
            if pattern == '**':
                # Include the current directory in **, if asked; by adding
                # an empty string as opposed to '.', we spare ourselves
                # having to deal with os.path.normpath() later.
                names = [''] if globstar_with_root else []
                for top, entries in self.walk(dirname):
                    _mkabs = lambda s: os.path.join(self.chop_dirname(dirname, top), s)
                    names.extend(map(_mkabs, entries))
                # Reset pattern so that fnmatch(), which does not understand
                # ** specifically, will only return a single group match.
                pattern = '*'
            else:
                names = self.listdir(dirname, prefix=get_magic_prefix(pattern))
        except os.error:
            return []

        if not _ishidden(pattern):
            # Remove hidden files by default, but take care to ensure
            # that the empty string we may have added earlier remains.
            # Do not filter out the '' that we might have added earlier
            names = filter(lambda x: not x or not _ishidden(x), names)
        return fnmatch.filter(names, pattern)


default_globber = Globber()
glob = default_globber.glob
iglob = default_globber.iglob
del default_globber


magic_check = re.compile('[*?[]')
magic_check_bytes = re.compile(b'[*?[]')

def get_magic_match(s):
    if isinstance(s, bytes):
        match = magic_check_bytes.search(s)
    else:
        match = magic_check.search(s)
    return match

def has_magic(s):
    return get_magic_match(s) is not None

def get_magic_prefix(s):
    match = get_magic_match(s)
    if match:
        return s[:match.start()]
    else:
        return s

def _ishidden(path):
    return path[0] in ('.', b'.'[0])

class S3Globber(Globber):
    curdir = ''            # The concept of '.' doesn't exist on S3
    fs_encoding = 'utf-8'  # S3 keynames are UTF-8

    def __init__(self, bucket):
        self.bucket = bucket
        self.memoized_queries = {}
        self.saved_keys = {}

    def _run_list(self, prefix):
        """Runs a list query. Uses memoized_queries where possible"""
        # AWS has weird semantics for listing '/'. We have to list
        # it as '' instead. This logic tends to conflict with glob2's
        # understanding of paths, so we normalize out this bump here.
        if prefix == '/':
            prefix = ''

        if prefix in self.memoized_queries:
            return self.memoized_queries[prefix]

        # print "RUNNING LIST QUERY list(prefix=%s, delimiter=%s)" % (prefix, '/')
        ret = [x for x in self.bucket.list(prefix=prefix, delimiter='/')]
        for x in ret:
            if not x.name.endswith('/'):
                self.saved_keys[x.name] = x
                self.memoized_queries[x.name] = [ x ]
        self.memoized_queries[prefix] = ret
        return ret

    def get_key(self, keyname):
        """Returns a key. Uses memoized_keys where possible"""
        keyname = self._normalize_unicode(keyname)

        if keyname in self.saved_keys:
            return self.saved_keys[keyname]
        else:
            key = self.bucket.get_key(keyname)
            if key:
                self.saved_keys[key.name] = key
            return key

    def _normalize_to_dirname(self, name):
        """boto's delimiter semantics won't list a directory
        if it doesn't end with a single '/', but this only
        applies to non-empty strings"""
        name = name.strip('/')
        name += '/'

        return name

    def _find_in_parent_dir(self, name):
        """Tries to take advantage of the fact that we probably queried the parent."""

        # We strip the '/' off of name if it's there
        # to help os.path.split()"""
        parent = self._normalize_to_dirname(os.path.split(name.strip('/'))[0])
        if not parent:
            return self.get_key('')
        if parent not in self.memoized_queries:
            iterator = self._run_list(name.rstrip('/'))
        else:
            iterator = self._run_list(parent)
        for x in iterator:
            if x.name == name:
                return x

    def listdir(self, dirname, prefix=''):
        dirname = self._normalize_unicode(dirname)
        prefix = self._normalize_unicode(prefix)

        normalized_dirname = self._normalize_to_dirname(dirname)

        if not self.isdir(normalized_dirname):
            raise OSError("%s is not a directory" % normalized_dirname)

        full_dirname = os.path.join(normalized_dirname, prefix)
        ret = [os.path.split(x.name.rstrip('/'))[1] for x in self._run_list(full_dirname) \
            if not (x.name.endswith('/') and self._normalize_to_dirname(x.name) == normalized_dirname)]
        # print "Listing dirname (%s -> %s) prefix (%s) result %s" % (dirname, normalized_dirname, prefix, ret)
        return ret

    def isdir(self, dirname):
        dirname = self._normalize_unicode(dirname)
        dirname = self._normalize_to_dirname(dirname)

        if dirname == '/':
            return True

        key = self._find_in_parent_dir(dirname)
        if key:
            ret = isinstance(key, boto.s3.prefix.Prefix)
        else:
            ret = False
        # print "isdir(%s) == %s" % (dirname, ret)
        return ret

    def islink(self, path):
        # Links don't exist on S3
        return False

    def exists(self, fname):
        fname = self._normalize_unicode(fname)

        if self.isdir(fname):
            ret = True
        else:
            ret = self._find_in_parent_dir(fname) is not None

        # print "exists(%s) == %s" % (fname, ret)
        return ret


class HDFSGlobber(Globber):
    curdir = ''
    fs_encoding = 'utf-8'

    def __init__(self, client):
        self.client = client
        self.memoized_queries = {}
        self.saved_fileinfo = {}

    def get_fileinfo(self, path):
        """Returns file info. Uses saved_Fileinfo where possible"""
        path = self._normalize_unicode(path)

        if path in self.saved_fileinfo:
            if 'etag' not in self.saved_fileinfo[path]:
                try:
                    checksuminfo = self.client.get_file_checksum(path)['FileChecksum']
                    self.saved_fileinfo[path]['etag'] = checksuminfo['bytes']
                except pywebhdfs.errors.PyWebHdfsException:
                    self.saved_fileinfo[path]['etag'] = None
            return self.saved_fileinfo[path]
        else:
            try:
                fileinfo = self.client.get_file_dir_status(path)['FileStatus']
                fileinfo['path'] = path
            except pywebhdfs.errors.PyWebHdfsException:
                return None

            try:
                checksuminfo = self.client.get_file_checksum(path)['FileChecksum']
                fileinfo['etag'] = checksuminfo['bytes']
            except pywebhdfs.errors.PyWebHdfsException:
                pass
            if fileinfo:
                self.saved_fileinfo[fileinfo['path']] = fileinfo
            return fileinfo

    def isdir(self, dirname):
        dirname = self._normalize_unicode(dirname)
        dirname = self._normalize_to_dirname(dirname)

        fileinfo = self._find_in_parent_dir(dirname)
        if fileinfo:
            return fileinfo['type'] == 'DIRECTORY'
        return False

    def islink(self, path):
        path = self._normalize_unicode(path)
        fileinfo = self._find_in_parent_dir(path)
        if fileinfo:
            return fileinfo['type'] == 'SYMLINK'
        return False

    def _normalize_to_dirname(self, name):
        name = name.rstrip('/')
        name += '/'
        return name

    def exists(self, fname):
        fname = self._normalize_unicode(fname)

        if self.isdir(fname):
            return True
        return self._find_in_parent_dir(fname) is not None

    def _run_list(self, prefix):
        """Runs a list query. Uses memoized_queries where possible"""
        if prefix in self.memoized_queries:
            return self.memoized_queries[prefix]

        try:
            ret = self.client.list_dir(prefix)['FileStatuses']['FileStatus']
        except pywebhdfs.errors.FileNotFound:
            return []

        self.memoized_queries[prefix] = ret
        for fileinfo in ret:
            # If we're listing, say, a directory called foo/ that contains a
            # file called bar, fileinfo will only contain a key called
            # 'pathSuffix' with the value 'bar'.  We thus add a key that
            # contains the full path.
            fileinfo['path'] = os.path.join(prefix, fileinfo['pathSuffix'])
            self.saved_fileinfo[fileinfo['path']] = fileinfo
        return ret

    def _find_in_parent_dir(self, name):
        """Tries to take advantage of the fact that we probably queried the parent."""
        name = self._normalize_unicode(name)
        parent = os.path.split(name.rstrip('/'))[0]
        if not parent:
            x = self.get_fileinfo(name.rstrip('/'))
            return x

        ret = self._run_list(parent)
        for fileinfo in ret:
            path = fileinfo['path']
            if fileinfo['type'] == 'DIRECTORY':
                if self._normalize_to_dirname(path) == self._normalize_to_dirname(name):
                    return fileinfo
            elif path == name:
                return fileinfo
        return None

    def listdir(self, dirname, prefix=''):
        dirname = self._normalize_unicode(dirname)

        normalized_dirname = self._normalize_to_dirname(dirname)

        if not self.isdir(normalized_dirname):
            raise OSError("%s is not a directory" % normalized_dirname)

        ret = []
        for x in self._run_list(normalized_dirname):
            # We don't want to include the directory that we're listing.
            if self._normalize_to_dirname(x['path']) != normalized_dirname:
                ret.append(os.path.split(x['path'].rstrip('/'))[1])
        return ret

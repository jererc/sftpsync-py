import os
import re
from stat import S_ISDIR
from datetime import datetime
import logging

import paramiko


logger = logging.getLogger(__name__)


class Sftp(object):
    def __init__(self, host, username, password=None, port=22,
                timeout=10, max_attempts=3, log_errors=True, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp = None
        self.logged = False
        for i in range(max_attempts):
            try:
                self.client.connect(host, port=port, username=username,
                        password=password, timeout=timeout, **kwargs)
                self.sftp = self.client.open_sftp()
                self.logged = True
                return
            except Exception, e:
                if i == max_attempts - 1 and log_errors:
                    logger.error('failed to connect to %s@%s:%s: %s', username, host, port, e)

    def _listdir(self, path):
        for file in self.sftp.listdir(path):
            yield os.path.join(path, file)

    def _makedirs(self, path):
        paths = []
        while path not in ('/', ''):
            paths.insert(0, path)
            path = os.path.dirname(path)

        for path in paths:
            try:
                self.sftp.lstat(path)
            except Exception:
                self.sftp.mkdir(path)

    def _walk_remote(self, path, topdown=True):
        for file in self._listdir(path):
            stat = self.sftp.lstat(file)

            if topdown:
                if not S_ISDIR(stat.st_mode):
                    yield 'file', file, stat
                else:
                    yield 'dir', file, stat
                    for res in self._walk_remote(file, topdown=topdown):
                        yield res

            else:
                if S_ISDIR(stat.st_mode):
                    for res in self._walk_remote(file, topdown=topdown):
                        yield res

                    yield 'dir', file, None
                    continue

                yield 'file', file, stat

    def _walk_local(self, path, topdown=True):
        for path, dirs, files in os.walk(path, topdown=topdown):
            for file in files:
                file = os.path.join(path, file)
                yield 'file', file, os.stat(file)
            for dir in dirs:
                dir = os.path.join(path, dir)
                yield 'dir', dir, os.stat(dir)

    def _validate_remote(self, file, src_stat):
        try:
            dst_stat = self.sftp.lstat(file)
        except Exception:
            return
        if dst_stat.st_mtime != src_stat.st_mtime:
            return
        if dst_stat.st_size != src_stat.st_size:
            return
        return True

    def _validate_local(self, file, src_stat):
        if not os.path.exists(file):
            return
        dst_stat = os.stat(file)
        if dst_stat.st_mtime != src_stat.st_mtime:
            return
        if dst_stat.st_size != src_stat.st_size:
            return
        return True

    def _store_remote(self, src, dst, src_stat):
        self.sftp.put(src, dst)
        self.sftp.utime(dst, (src_stat.st_atime, src_stat.st_mtime))

    def _store_local(self, src, dst, src_stat):
        self.sftp.get(src, dst)
        os.utime(dst, (src_stat.st_atime, src_stat.st_mtime))

    def _delete_remote(self, path, files, dry=False):
        for type, file, stat in self._walk_remote(path, topdown=False):
            if file not in files[type]:
                try:
                    if not dry:
                        if type == 'file':
                            self.sftp.remove(file)
                        else:
                            self.sftp.rmdir(file)

                    logger.debug('removed %s', file)
                except Exception, e:
                    logger.debug('failed to remove remote file %s: %s', file, e)

    def _delete_local(self, path, files, dry=False):
        for type, file, stat in self._walk_local(path, topdown=False):
            if file not in files[type]:
                try:
                    if not dry:
                        if type == 'file':
                            os.remove(file)
                        else:
                            os.rmdir(file)

                    logger.debug('removed %s', file)
                except Exception, e:
                    logger.debug('failed to remove local file %s: %s', file, e)

    def _get_filters(self, filters):
        res = []
        for filter in filters:
            res.append(re.compile(filter))
        return res

    def _validate_filters(self, val, include, exclude):
        for re_ in include:
            if not re_.search(val):
                return False
        for re_ in exclude:
            if re_.search(val):
                return False
        return True

    def sync(self, src, dst, download=True, include=None, exclude=None, delete=False, dry=False):
        '''Sync directories.

        :param src: source directory
        :param dst: destination directory
        :param download: True to sync from a remote source to a local destination,
            else from a local source to a remote destination
        :param include: list of regex patterns the source files must match
        :param exclude: list of regex patterns the source files must not match
        :param delete: remove destination files and directories not present at source
            or filtered by the include/exlude patterns
        '''
        if not exclude:
            exclude = []
        if not include:
            include = []
        if exclude:
            exclude = self._get_filters(exclude)
        if include:
            include = self._get_filters(include)

        if src.endswith('/') != dst.endswith('/'):
            dst = os.path.join(dst, os.path.basename(src.rstrip('/')))
        src = src.rstrip('/')
        re_base = re.compile(r'^%s/' % src)
        if not src:
            src = '/'

        if download:
            callable_walk = self._walk_remote
            callable_delete = self._delete_local
            callable_validate = self._validate_local
            callable_store = self._store_local
            if not dry and not os.path.exists(dst):
                os.makedirs(dst)
        else:
            callable_walk = self._walk_local
            callable_delete = self._delete_remote
            callable_validate = self._validate_remote
            callable_store = self._store_remote
            if not dry:
                self._makedirs(dst)

        started = datetime.utcnow()
        total_size = 0
        dst_list = {'file': [], 'dir': []}

        for type, file, stat in callable_walk(src):
            file_ = re_base.sub('', file)
            if not self._validate_filters(file_, include, exclude):
                logger.debug('filtered %s', file)
                continue

            dst_file = os.path.join(dst, file_)
            dst_list[type].append(dst_file)

            if type == 'dir':
                if download:
                    if not os.path.exists(dst_file):
                        os.makedirs(dst_file)
                        logger.debug('created destination directory %s', dst_file)
                else:
                    try:
                        self.sftp.lstat(dst_file)
                    except Exception:
                        self.sftp.mkdir(dst_file)
                        logger.debug('created destination directory %s', dst_file)

            elif type == 'file':
                if not callable_validate(dst_file, stat):
                    if not dry:
                        callable_store(file, dst_file, stat)
                    total_size += stat.st_size
                    logger.debug('copied %s to %s', file, dst_file)

        if delete:
            callable_delete(dst, dst_list, dry=dry)

        logger.debug('transferred %s bytes in %s', total_size, datetime.utcnow() - started)

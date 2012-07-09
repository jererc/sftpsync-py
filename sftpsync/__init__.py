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
        try:
            for file in self.sftp.listdir(path):
                yield os.path.join(path, file)
        except Exception, e:
            logger.info('failed to list %s: %s', path, e)

    def _walk_remote(self, path, topdown=True):
        for file in self._listdir(path):
            stat = self.sftp.lstat(file)

            if not S_ISDIR(stat.st_mode):
                yield 'file', file, stat
            else:
                if topdown:
                    yield 'dir', file, stat
                    for res in self._walk_remote(file, topdown=topdown):
                        yield res
                else:
                    for res in self._walk_remote(file, topdown=topdown):
                        yield res
                    yield 'dir', file, None

    def _walk_local(self, path, topdown=True):
        for path, dirs, files in os.walk(path, topdown=topdown):
            for file in files:
                file = os.path.join(path, file)
                yield 'file', file, os.stat(file)
            for dir in dirs:
                dir = os.path.join(path, dir)
                yield 'dir', dir, os.stat(dir)

    def _walk(self, *args, **kwargs):
        remote = kwargs.pop('remote', False)
        if remote:
            return self._walk_remote(*args, **kwargs)
        else:
            return self._walk_local(*args, **kwargs)

    def _makedirs_dst(self, path, remote=True, dry=False):
        if remote:
            paths = []
            while path not in ('/', ''):
                paths.insert(0, path)
                path = os.path.dirname(path)

            for path in paths:
                try:
                    self.sftp.lstat(path)
                except Exception:
                    if not dry:
                        self.sftp.mkdir(path)
                    logger.debug('created destination directory %s', path)
        else:
            if not os.path.exists(path):
                if not dry:
                    os.makedirs(path)
                logger.debug('created destination directory %s', path)

    def _validate_src(self, file, include, exclude):
        for re_ in include:
            if not re_.search(file):
                return False
        for re_ in exclude:
            if re_.search(file):
                return False
        return True

    def _validate_dst(self, file, src_stat, remote=True):
        if remote:
            try:
                dst_stat = self.sftp.lstat(file)
            except Exception:
                return
        else:
            if not os.path.exists(file):
                return
            dst_stat = os.stat(file)

        if dst_stat.st_mtime != src_stat.st_mtime:
            return
        if dst_stat.st_size != src_stat.st_size:
            return
        return True

    def _save(self, src, dst, src_stat, remote=True):
        if remote:
            logger.info('copying %s to %s@%s:%s', src, self.username, self.host, dst)
            self.sftp.put(src, dst)
            self.sftp.utime(dst, (src_stat.st_atime, src_stat.st_mtime))
        else:
            logger.info('copying %s@%s:%s to %s', self.username, self.host, src, dst)
            self.sftp.get(src, dst)
            os.utime(dst, (src_stat.st_atime, src_stat.st_mtime))

    def _delete_dst(self, path, files, remote=True, dry=False):
        if remote:
            callables = {'file': self.sftp.remove, 'dir': self.sftp.rmdir}
        else:
            callables = {'file': os.remove, 'dir': os.rmdir}

        for type, file, stat in self._walk(path, topdown=False, remote=remote):
            if file not in files[type]:
                if not dry:
                    try:
                        callables[type](file)
                    except Exception, e:
                        logger.debug('failed to remove %s: %s', file, e)
                        continue

                logger.debug('removed %s', file)

    def _get_filters(self, filters):
        if not filters:
            return []
        return [re.compile(f) for f in filters]

    def sync(self, src, dst, download=True, include=None, exclude=None, delete=False, dry=False):
        '''Sync files and directories.

        :param src: source directory
        :param dst: destination directory
        :param download: True to sync from a remote source to a local destination,
            else sync from a local source to a remote destination
        :param include: list of regex patterns the source files must match
        :param exclude: list of regex patterns the source files must not match
        :param delete: remove destination files and directories not present
            at source or filtered by the include/exlude patterns
        '''
        include = self._get_filters(include)
        exclude = self._get_filters(exclude)

        if src.endswith('/') != dst.endswith('/'):
            dst = os.path.join(dst, os.path.basename(src.rstrip('/')))
        src = src.rstrip('/')
        re_base = re.compile(r'^%s/' % src)
        if not src:
            src = '/'

        self._makedirs_dst(dst, remote=not download, dry=dry)

        started = datetime.utcnow()
        total_size = 0
        dst_list = {'file': [], 'dir': []}

        for type, file, stat in self._walk(src, remote=download):
            file_ = re_base.sub('', file)
            if not self._validate_src(file_, include, exclude):
                logger.debug('filtered %s', file)
                continue

            dst_file = os.path.join(dst, file_)
            dst_list[type].append(dst_file)

            if type == 'dir':
                self._makedirs_dst(dst_file, remote=not download, dry=dry)
            elif type == 'file':
                if not self._validate_dst(dst_file, stat, remote=not download):
                    if not dry:
                        self._save(file, dst_file, stat, remote=not download)
                    total_size += stat.st_size
                    logger.debug('copied %s to %s', file, dst_file)

        if delete:
            self._delete_dst(dst, dst_list, remote=not download, dry=dry)

        logger.debug('transferred %s bytes in %s', total_size, datetime.utcnow() - started)

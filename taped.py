import subprocess as sp
import os, sys
import time


def run_cmd(cmd):
    # Run cmd through the shell so you can pipe and whatever else.
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    out, err = p.communicate()
    return p.returncode, out, err

class TapeDrive:
    def __init__(self, nst_addr, ssh_cmd=None):
        self.nst = nst_addr
        self.mt = '/bin/mt -f %s ' % self.nst
        self.ssh_cmd = ssh_cmd

    def rewind(self):
        return run_cmd(self.mt + 'rewind')

    def status(self):
        #File number=0, block number=0, partition=0.
        code, out, err = run_cmd(self.mt + 'status')
        assert(code == 0)
        tokens = [tk.strip().replace('.','').replace(' ','_').lower() 
                  for tk in out.split('\n')[1].split(',')]
        tokens = [x.split('=') for x in tokens]
        return dict([(x[0], int(x[1])) for x in tokens])

    def tape_checksums(self):
        """Read tar archive from the current position on the tape;
        extract files and pass them through md5sum.  Output is a list
        of tuples (md5sum, filename)."""
        # Note that a simple cat here sometimes crashes, roughly once
        # it has passed data equal to the size of system RAM.  dd does
        # better.
        code, out, err = run_cmd(
            'dd if=%s bs=512k | tar -x ' % self.nst + 
            '--to-command=\'sh -c "md5sum | sed \\"s|-|\$TAR_FILENAME|\\""\'')
        assert(code == 0)
        return [line.strip().split() for line in out.split('\n')
                if line.strip() != '']

    def tape_files(self):
        """Read tar archive from current position on the tape and get
        list of files.  Note this includes directories and symlinks
        (unlike tape_checksums)."""
        code, out, err = run_cmd(
            'dd if=%s bs=512k | tar -t ' % self.nst)
        assert(code == 0)
        return [line.strip() for line in out.split('\n')
                if line.strip() != '']

    def goto(self, file_number):
        here = self.status()['file_number']
        assert file_number >= 0
        if file_number == 0:
            return self.rewind()
        delta = file_number - here
        if delta > 0:
            code, out, err = run_cmd(self.mt + 'fsf %i' % delta)
        if delta <= 0:
            code, out, err = run_cmd(self.mt + 'bsfm %i' % (1 - delta))
        assert(code == 0)  # goto failed
        return code, out, err

    def remote_target_info(self, fpath, excluded_subdirs=[],
                           verbosity=0, recursion=0):
        """
        Connect to the remote (possibly multiple times) and determine
        file sizes and md5sums of all items below fpath.  Returns list
        of tuples (filename, file_size_kB, md5sum).  For symlinks,
        file_size is 0 and md5sum is the string 'symlink'.
        """
        fpath = os.path.normpath(fpath)
        if verbosity:
            print time.asctime(), 'Scanning [depth=%i] %s' % (recursion, fpath)
        info = {}
        # Get file sizes
        if verbosity:
            print time.asctime(), 'Getting file sizes...'
        find_cmd = 'find %s -maxdepth 1 -mindepth 1' % fpath
        code, out, err = run_cmd(
            '%s "%s | xargs --no-run-if-empty -d \'\\n\' du"' %
            (self.ssh_cmd, find_cmd + ' -type f'))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        assert(code==0) # find | du
        lines = out.split('\n')
        for line in out.split('\n'):
            if line.strip() == '': continue
            size, filename = line.split('\t')
            info[filename] = [int(size)]
        if verbosity:
            print time.asctime(), ' ... retrieved %i files (total %.1f MB)' % \
                (len(info.keys()), sum([x[0] for x in info.values()]) / 1e3)
        # And the md5sums
        if verbosity:
            print time.asctime(), 'Getting md5sums...'
        code, out, err = run_cmd(
            '%s "%s | xargs --no-run-if-empty -d \'\\n\' md5sum"' %
            (self.ssh_cmd, find_cmd + ' -type f'))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        for line in out.split('\n'):
            if line.strip() == '': continue
            assert(line[32:34] == '  ')
            md5, filename = line[:32], line[34:].strip()
            assert filename in info
            info[filename].append(md5)
        data = [(k,) + tuple(v) for k, v in info.items()]
        if verbosity:
            print time.asctime(), ' ... done.'
        # And symlinks :P
        if verbosity:
            print time.asctime(), 'Getting symlinks...'
        code, out, err = run_cmd(
            '%s "%s"' % (self.ssh_cmd, find_cmd + ' -type l'))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        for line in out.split('\n'):
            if line.strip() == '': continue
            filename = line
            info[filename] = [0, 'symlink']
        # One big list.
        data = [(k,) + tuple(v) for k, v in info.items()]
        if verbosity:
            print time.asctime(), ' ... done.'
        # But now descend to subdirs...
        if verbosity:
            print time.asctime(), 'Getting subdir list...'
        code, out, err = run_cmd(
            '%s "%s"' % (self.ssh_cmd, find_cmd + ' -type d'))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        subdirs = [str(x) for x in out.split('\n') if len(x) != 0]
        if verbosity:
            print time.asctime(), 'Descending into %i subdirs...' % len(subdirs)
        for d in subdirs:
            if d not in excluded_subdirs:
                data += self.remote_target_info(d, excluded_subdirs,
                                                verbosity=verbosity,
                                                recursion=recursion+1)
            elif verbosity:
                print(' ... excluded %s' % d)
        return sorted(data)

    def remote_checksums(self, fpath):
        fpath = os.path.normpath(fpath)
        code, out, err = run_cmd(
            '%s md5sum %s/*' % (self.ssh_cmd, fpath))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        return [x.split() for x in out.split('\n')]

    def archive_remote(self, fpath, exclude_patterns=[]):
        """
        Copies a target to tape, over ssh, via tar.  Returns (code,
        out, err) which are the exit code (integer), stdout and stderr
        from the command.  out will probably be None.  code will be 0 on
        success.  err should be presented to the user if code != 0.
        """
        fpath = os.path.normpath(fpath)
        print 'Archiving: %s' % fpath
        # Modifiers to exclude handled children.
        ex_pats = ' '.join(['--exclude="%s"' % p for p in exclude_patterns])
        code, out, err = run_cmd(
            '%s tar -c %s %s > %s' % (self.ssh_cmd, ex_pats, fpath, self.nst))
        # Only proceed if code is 0!
        return code, out, err


class TapeDriveEmulator(TapeDrive):
    def __init__(self, tar_dir, ssh_cmd=None):
        self.tar_dir = tar_dir
        self.ssh_cmd = ssh_cmd
        self.goto(0)
    def status(self):
        return {'file_number': self._position}
    def goto(self, file_number):
        self._position = file_number
        self.nst = os.path.join(self.tar_dir, 'file%05i.tar' % self._position)
        return 0, '', ''


if __name__ == '__main__':
    td = TapeDrive('/dev/nst0', '')
    print 'td.status() says... '
    print td.status()
    print


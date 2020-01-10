import subprocess as sp
import os, sys

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

    def remote_target_info(self, fpath):
        """
        Connect to the remote (possibly multiple times) and determine
        file sizes and md5sums of all items below fpath.  Returns list
        of tuples (filename, file_size_kB, md5sum).  For symlinks,
        file_size is 0 and md5sum is the string 'symlink'.
        """
        fpath = os.path.normpath(fpath)
        info = {}
        find_cmd = 'find %s -type f' % fpath
        # Get file sizes
        code, out, err = run_cmd(
            '%s "%s | xargs --no-run-if-empty du"' % (self.ssh_cmd, find_cmd))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        assert(code==0) # find | du
        print 'Getting file sizes...'
        for line in out.split('\n'):
            if line.strip() == '': continue
            size, filename = line.split()
            info[filename] = [int(size)]
        # And the md5sums
        print 'Getting md5sums...'
        code, out, err = run_cmd(
            '%s "%s | xargs --no-run-if-empty md5sum"' % (self.ssh_cmd, find_cmd))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        for line in out.split('\n'):
            if line.strip() == '': continue
            md5, filename = line.split()
            assert filename in info
            info[filename].append(md5)
        data = [(k,) + tuple(v) for k, v in info.items()]
        # And symlinks :P
        print 'Getting symlinks...'
        find_cmd = 'find %s -type l' % fpath
        code, out, err = run_cmd(
            '%s "%s"' % (self.ssh_cmd, find_cmd))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        for line in out.split('\n'):
            if line.strip() == '': continue
            filename = line
            info[filename] = [0, 'symlink']
        # One big list.
        data = [(k,) + tuple(v) for k, v in info.items()]
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
        fpath = os.path.normpath(fpath)
        print 'Archiving: %s' % fpath
        # Modifiers to exclude handled children.
        ex_pats = ' '.join(['--exclude="%s"' % p for p in exclude_patterns])
        code, out, err = run_cmd(
            '%s tar -c %s %s > %s' % (self.ssh_cmd, ex_pats, fpath, self.nst))
        assert(code == 0)
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


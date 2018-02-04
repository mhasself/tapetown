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
        fpath = os.path.normpath(fpath)
        info = {}
        find_cmd = 'find %s -type f' % fpath
        # Get file sizes
        code, out, err = run_cmd(
            '%s "%s | xargs du"' % (self.ssh_cmd, find_cmd))
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
            '%s "%s | xargs md5sum"' % (self.ssh_cmd, find_cmd))
        if code != 0:
            print 'Error!', out, err
            raise RuntimeError
        for line in out.split('\n'):
            if line.strip() == '': continue
            md5, filename = line.split()
            assert filename in info
            info[filename].append(md5)
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

    def archive_remote(self, fpath):
        fpath = os.path.normpath(fpath)
        print 'Archiving: %s' % fpath
        code, out, err = run_cmd(
            '%s tar -c %s > %s' % (self.ssh_cmd, fpath, self.nst))
        assert(code == 0)
        return code, out, err

if __name__ == '__main__':
    td = TapeDrive('/dev/nst0', '')
    print 'td.status() says... '
    print td.status()
    print


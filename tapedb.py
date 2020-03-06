import sqlite3
import imp, os, sys, glob
import subprocess as sp
import time, calendar


TABLE_DEFS = {
    'tapes': [
        "`id` integer primary key autoincrement",
        "`name` varchar(256) unique",
        "`serial` varchar(256) unique",
        "`status` varchar(16)",
        "`online` integer not null default 0",
        ],
    'files': [
        "`id` integer primary key autoincrement",
        "`target_id` integer",
        "`name` varchar(512)",
        "`md5sum` varchar(32)",
        "`size_kb` integer",
        "constraint file_on_target UNIQUE (target_id, name)"
        ],
    'targets': [
        "`id` integer primary key autoincrement",
        "`name` varchar(2048) unique",
        "`scanned` integer default 0",
        "`parent_id` integer default null"
        ],
    'backups': [
        "`id` integer primary key autoincrement",
        "`target_id` integer",
        "`tape_id` integer",
        "`file_number` integer",
        "`status` varchar(16)",
        ],
}

defaults = {
    'tape_size_MB': 6000000
}


class TapeDB:
    VALID_TAPE_STATUS = ['open', 'closed', 'garbage']

    def __init__(self, db_filename, init_tables=True):
        self.conn = sqlite3.connect(db_filename)
        self.conn.row_factory = sqlite3.Row  # access columns by name
        if init_tables:
            self.create_tables()

    # Generic.
    def create_tables(self):
        c = self.conn.cursor()
        for table,tdef in TABLE_DEFS.items():
            if table[0] == '#': continue
            q = ('create table if not exists `%s` (' % table  +
                 ','.join(tdef) + ')')
            c.execute(q)
        self.conn.commit()

    def drop_table(self, name):
        c = self.conn.cursor()
        c.execute('drop table %s' % name)

    # Work with targets.
    def target_create(self, fpath, ignore_duplicate=True, assign_parent=True):
        fpath = os.path.normpath(fpath)
        c = self.conn.cursor()
        parent_id = self.get_target_parent(fpath)
        try:
            c.execute('insert into targets (name,parent_id) values (?,?)',
                      (fpath,parent_id))
            self.conn.commit()
        except sqlite3.IntegrityError as e: # duplicate key
            if not ignore_duplicate:
                raise e
        c.execute('select id from targets '
                  'where name=?', (fpath,))
        return c.fetchone()[0]
        
    def add_files(self, file_data, prefix=None):
        """Introduce files to the database.  The file_data is a list of
        tuples of the form
        
          (name, size, md5sum)

        The name field contains the full file path.  If prefix is
        specified, then the files will be stored in the database
        relative to a target called prefix.  In this case, all file
        paths must actually overlap with the prefix.  If prefix is
        None, the full path to the file (excluding the filename) will
        be used for the target.

        Targets will be created, as needed, to associate with the file
        info.

        Note that if a BackupItem already exists for the target, the
        add will be blocked.  Add all files for a target before you
        "assign" it to a tape.

        """
        if prefix is not None:
            print 'Adding files to target %s' % prefix
            target_id = self.target_create(prefix)
            if len(BackupItem.for_target(self, target_id)) != 0:
                raise RuntimeError, 'Backup configurations exist for target: %s' % prefix

        base = None
        c = self.conn.cursor()
        for row in file_data:
            name, size, md5 = row
            if prefix is not None:
                assert(name.startswith(prefix))
                name = name[len(prefix):]
                while name[0] == '/':
                    name = name[1:]
            else:
                _base, name = os.path.split(name)
                if base != _base:
                    base = _base
                    print 'Adding files to target %s' % base
                    target_id = self.target_create(base)
                    if len(BackupItem.for_target(self, target_id)) != 0:
                        raise RuntimeError, 'Backup configurations exist for target: %s' % base
            c.execute('insert or replace into files (target_id, name, md5sum, size_kb) values '
                      '(?,?,?,?)', (target_id, name, md5, size))
        self.conn.commit()

    def get_target_id(self, target):
        if isinstance(target, int):
            return target
        c = self.conn.cursor()
        c.execute('select id from targets where name=?', (target, ))
        row = c.fetchone()
        if row == None:
            return None
        return int(row[0])

    def get_target_parent(self, target_name):
        """Checks if target has a parent in the database already, and
        if so returns the id."""
        test_path = target_name
        while True:
            test_path = os.path.split(test_path)[0]
            if test_path in ['/', '']: break
            target_id = self.get_target_id(test_path)
            if target_id is not None:
                return target_id
        return None

    def get_target_info(self, target_name):
        c = self.conn.cursor()
        if isinstance(target_name, int):
            c.execute('select name, id, scanned, parent_id from targets '
                      'where id=?', (target_name, ))
        else:
            c.execute('select name, id, scanned, parent_id from targets '
                      'where name=?', (target_name, ))
        rows = c.fetchall()
        if len(rows) == 0:
            return None
        return dict([(k, v) for k, v in
                zip(['name', 'id', 'scanned', 'parent_id'], rows[0])])

    def set_target_scanned(self, target_name):
        target_id = self.get_target_id(target_name)
        c = self.conn.cursor()
        c.execute('update targets set scanned=1 where id=?', (target_id,))
        self.conn.commit()

    def reassign_from_parent(self, child_target_name):
        """
        A target is the child of another target if it is a
        sub-directory (at any level) of that parent.  This function
        gets or creates the target_id for the child_target_name, and
        reassigns any files that lie in the child's tree from the
        parent to the child.

        When building the target/file database, it's important to add
        parent directories before children; i.e. '/root', then
        '/root/dir1', and then '/root/dir1/some/deep/other/dir'.
        """
        parent_id = self.get_target_parent(child_target_name)
        c = self.conn.execute('select name from targets where id=?',
                              (parent_id,))
        parent_name = c.fetchone()[0]
        child_id = self.get_target_id(child_target_name)
        assert(parent_id is not None)
        if child_id is None:
            child_id = self.target_create(child_target_name)
        assert(child_id is not None)
        # Make sure the child target points to the parent, so its path
        # can easily be excluded when making the parent's archive.
        self.conn.execute('update targets set parent_id=? where id=?',
                          (parent_id, child_id))
        # What's the path delta between parent and child?
        assert(child_target_name.startswith(parent_name))
        path_delta = child_target_name[len(parent_name):]
        assert path_delta[0] == '/' and path_delta[-1] != '/'
        path_delta = path_delta[1:] + '/'
        c = self.conn.execute(
            'update files set target_id=?, name=substr(name,?) '
            'where target_id=? and substr(name,1,?)==?',
            (child_id, len(path_delta)+1,
             parent_id, len(path_delta), path_delta))
        n = c.rowcount
        self.conn.commit()
        return n

    def get_excluded_subdirs(self, target_id):
        """Return the names of targets that are direct children of
        this target.  This is equivalent to the list of sub-dirs that
        should be excluded from this target's tar archive creation
        command."""
        target_id = self.get_target_id(target_id)
        c = self.conn.execute('select name from targets where parent_id=?',
                              (target_id,))
        return [r[0] for r in c]

    def get_unassigned_targets(self, get_sizes=False):
        """
        Find targets that do not have an associated entry in backups.
        """
        c = self.conn.cursor()
        if get_sizes:
            c.execute('select T.id, T.name, sum(F.size_kb) '
                      'from targets as T left join backups as B '
                      'on T.id=B.target_id '
                      'left join files F on T.id=F.target_id '
                      'where B.target_id is null '
                      'group by T.id, T.name '
                      'order by T.name')
        else:
            c.execute('select T.id, T.name '
                      'from targets as T left join backups as B '
                      'on T.id=B.target_id '
                      'where B.target_id is null '
                      'order by T.name')
        return [r for r in c]

    def find_file(self, filename):
        c = self.conn.cursor()
        c.execute('select T.name as target_name, F.name as filename, '
                  'F.md5sum as md5sum, P.name as tape_name, '
                  'B.status as backup_status, B.file_number as tape_filenum, '
                  'P.name as tape_name '
                  'from targets as T join files as F join backups as B join tapes as P '
                  'where T.id=B.target_id and F.target_id=T.id and P.id=B.tape_id '
                  'and F.name = ?',
                  (filename,))
        return [r for r in c]

    def get_files_in_target(self, target_id):
        target_id = self.get_target_id(target_id)
        c = self.conn.cursor()
        c.execute('select T.name,F.name,F.md5sum '
                  'from targets as T join files as F '
                  'where F.target_id=T.id',
                  'and T.id = ?',
                  (target_id,))
        return [r for r in c]

    # Work with tapes.
    def get_tape_id(self, tape_name):
        c = self.conn.cursor()
        c.execute('select id from tapes '
                  'where name=?', (tape_name,))
        return c.fetchone()[0]

    def get_active_tape(self):
        c = self.conn.cursor()
        c.execute('select id,name from tapes '
                  'where online=1')
        row = c.fetchone()
        if row is None:
            return None, None
        return row

    def set_active_tape(self, tape_name, commit=True):
        c = self.conn.cursor()
        c.execute('update tapes set online=0')
        c.execute('update tapes set online=1 where name=?', (tape_name,))
        if commit:
            self.conn.commit()


    def create_tape(self, tape_name, serial, status=None, online=None):
        if status == None:
            status = 'open'
        assert(status in self.VALID_TAPE_STATUS)
        c = self.conn.cursor()
        c.execute('insert into tapes (name,serial,status) values (?,?,?)',
                  (tape_name,serial,status))
        if online:
            self.set_active_tape(tape_name, commit=False)
        self.conn.commit()
        return self.get_tape_id(tape_name)

    def close_tape(self, tape_id):
        if isinstance(tape_id, basestring):
            tape_id = self.get_tape_id(tape_id)
        c = self.conn.cursor()
        c.execute('update tapes set status=?,online=? where id=?',
                  ('closed', 0, tape_id))
        self.conn.commit()

    def get_tape_info(self, tape_name=None):
        c = self.conn.cursor()
        q = 'select id,name,serial,status,online from tapes '
        if tape_name is None:
            c.execute(q)
        else:
            c.execute(q + 'where name=?', (tape_name,))
        return [dict([(k,r[k]) for k in ['id','name','serial','status','online']]) for r in c]

    def get_tape_work(self, tape_id, status):
        if isinstance(status, basestring):
            status = [status]
        if isinstance(tape_id, basestring):
            tape_id = self.get_tape_id(tape_id)
        c = self.conn.cursor()
        qstr = '('+','.join(['?' for _ in status]) + ')'
        c.execute(('select B.id as id,tape_id,file_number,status,target_id,T.name as name '
                   'from backups as B join targets as T on B.tape_id=T.id '
                   'where tape_id=? '
                   'and status in ' + qstr + ' '
                   'order by T.name'),
                  (tape_id, )+tuple(status))
        return [BackupItem.from_row(self, row) for row in c]

    def get_tape_report(self, tape_id):
        """
        Returns list of intervals of tape space that are either
        "recorded" or "confirmed".  This will normally be of the form, e.g.:

            [[u'confirmed', 0, 9],
             [u'recorded', 10, 15]]
            
        Note the interval end points are inclusive.  This can be used
        to determine what records need confirmation, what gaps exist,
        or where to add the next backup.
        """
        if isinstance(tape_id, basestring):
            tape_id = self.get_tape_id(tape_id)
        c = self.conn.cursor()
        c.execute('select * from backups where tape_id=? and '
                  'status in ("confirmed", "recorded") '
                  'order by file_number', 
                  (tape_id,))
        stretches = []
        for row in c:
            new_thing = [row['status'], row['file_number'], row['file_number']]
            if (len(stretches) == 0 or 
                stretches[-1][0] != new_thing[0] or 
                stretches[-1][2]+1 != new_thing[1]):
                stretches.append(new_thing)
            else:
                stretches[-1][2] = new_thing[2]
        return stretches


class TargetInfo:
    name = None
    size_kb = None
    files = None


class BackupItem:
    """
    tape_id and file_number indicate the tape and file_number on the
    tape at which the data will be or are or were supposed to be
    stored.

    The "status" takes one of the following values:

    "new"
    
        A newly created object.  Neither tape_id nor file_number
        should be taken seriously.  Overall you should be suspicious.
        To convey that a target does not have an assignment, destroy
        the BackupItem rather than setting status='new'.

    "assigned"

        Backup has been tentatively assigned to a tape.  tape_id is
        meaningful, file_number is not.

    "recorded"

        Backup has been written to a particular place on the tape.
        Both tape_id and file_number are now meaningful.

    "confirmed"

        Like "recorded", but the backup has been checksummed.
    """
    VALID_STATUS = ['new', 'assigned', 'recorded', 'confirmed']

    @classmethod
    def new(cls, db, target):
        self = cls()
        self._id = None
        self.db = db
        target = db.get_target_id(target)
        assert(target is not None)
        self.target_id = target
        self.tape_id = None
        self.file_number = -1
        self.status = 'new'
        self.commit()
        return self

    @classmethod
    def for_target(cls, db, target):
        target = db.get_target_id(target)
        if target is None:
            return []
        c = db.conn.cursor()
        c.execute('select id, tape_id, file_number, status, target_id from backups '
                  'where target_id=?', (target, ))
        return [cls.from_row(db, row) for row in c]

    @classmethod
    def by_tape_id(cls, db, tape_name, file_number):
        tape_id = db.get_tape_id(tape_name)
        c = db.conn.cursor()
        c.execute('select id, tape_id, file_number, status, target_id from backups '
                  'where tape_id=? and file_number=?', (tape_id, file_number))
        return [cls.from_row(db, row) for row in c]

    @classmethod
    def from_row(cls, db, row):
        self = cls()
        self.db = db
        self._id, self.tape_id, self.file_number, self.status, self.target_id = [
            row[k] for k in ['id', 'tape_id', 'file_number', 'status', 'target_id']]
        return self

    def commit(self):
        assert self.status in self.VALID_STATUS
        c = self.db.conn.cursor()
        data = (self.target_id, self.tape_id, self.file_number, self.status)
        if self._id is None:
            c.execute('insert into backups '
                      '(target_id, tape_id, file_number, status) '
                      'values (?,?,?,?)', data)
            self._id = c.lastrowid
        else:
            c.execute('update backups set target_id=?, tape_id=?, file_number=?, status=? '
                      'where id=%s' % self._id, data)
        self.db.conn.commit()

    def destroy(self):
        c = self.db.conn.cursor()
        if self._id is None:
            return
        c.execute('delete from backups where id=?', (self._id,))
        self.db.conn.commit()
        self._id = None

    def get_target_info(self):
        out = TargetInfo()
        c = self.db.conn.cursor()
        c.execute('select name from targets where id=?', (self.target_id,))
        out.name = c.fetchone()[0]
        c.execute('select name, size_kb, md5sum from files '
                  'where target_id=? '
                  'order by name',
                  (self.target_id, ))
        out.files = [tuple(r) for r in c]
        out.size_kb = sum([x[1] for x in out.files])
        return out

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
    def target_create(self, fpath, ignore_duplicate=True):
        fpath = os.path.normpath(fpath)
        c = self.conn.cursor()
        try:
            c.execute('insert into targets (name) values (?)', (fpath,))
            self.conn.commit()
        except sqlite3.IntegrityError as e: # duplicate key
            if not ignore_duplicate:
                raise e
        c.execute('select id from targets '
                  'where name=?', (fpath,))
        return c.fetchone()[0]
        
    def add_files(self, file_data):
        """
        Introduce files to the database, creating new targets as
        necessary.  The file_data is a list of tuples of the form
        
        (name, size, md5sum)

        The name contains the full file path, the root of which is
        the "target" this file will be associated with.

        Note that if a BackupItem already exists for the target, the
        add will be blocked.  Add all files for a target before you
        "assign" it to a tape.
        """

        base = None
        c = self.conn.cursor()
        for row in file_data:
            name, size, md5 = row
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

    def get_unassigned_targets(self):
        c = self.conn.cursor()
        c.execute('select T.id, T.name from targets as T left join backups as B '
                  'on T.id=B.target_id where B.target_id is null '
                  'order by T.name')
        return [r for r in c]

    def find_file(self, filename):
        c = self.conn.cursor()
        c.execute('select T.name,F.name,F.md5sum,P.name,B.status,B.file_number '
                  'from targets as T join files as F join backups as B join tapes as P '
                  'where T.id=B.target_id and F.target_id=T.id and P.id=B.tape_id '
                  'and F.name = ?',
                  (filename,))
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

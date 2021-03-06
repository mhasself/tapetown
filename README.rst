==========================
TAPE ARCHIVING OF ACT DATA
==========================

Overview
========

This code is for creating a tape copy of a data archive.  Individual
directories in the source file system are copied in their entirety
into single "files" on the tape.  An sqlite database is used to keep
track of tapes, directories (referred to as targets) and the checksums
of all individual files.

Currently it's assumed that:
- you don't need to compress the data on the way to tape.
- you can pull the data over an ssh connection.

In addition to the copy job, the code also supports (and encourages) a
"confirmation" run, where the data are read back from the tape and
compared against the checksums in the database.


Backup Workflow
===============

Once a tape is prepared, one proceeds through the following
steps:  import, assign, archive, confirm.

All operations are accomplished with the main python script "tapeop".
Currently, stringing together several non-atomic operations (such as
archiving or confirming a long list of targets) can be done using
tape_batch.bash.

Note that "import" introduces targets into the database without
associating them with a particular tape.  Even once an association of
a target to a particular tape has been made, it can be changed later
provided that the target was never written to that tape.  So in
practice a workflow is more like:

- import a bunch of targets
- open tape1
- assign files to tape1
- archive until tape1 is full (leaves some files outstanding)
- confirm files on tape1
- close tape1 (this causes outstanding files to be unassigned)
- open tape2
- assign files to tape2
- archive until tape2 is full or all files are backed up
- confirm
- etc.


Configuration file
==================

The tapeop script will look for ``tape.conf`` in the current
directory.  See ``tape.conf.ex`` in this repository for an example of
what that should look like.  This is an ini-style config file with
keys under the ``[default]`` heading.  The role of each key is:

* ``database_file``: The filename for the sqlite database file.  E.g.,
  ``run1.sqlite``.
* ``tape_device``: The device node for the tape device.  E.g.,
  ``/dev/nst0``.  A single tape drive is typically exposed on multiple
  different device nodes, each having different basic configuration
  options.  You should make sure to use the non-rewinding device.  You
  should also choose whether to use tape drive native compression, or
  not.
* ``ssh_command``: An ssh command that connects to the remote machine
  (from which data will be copied), that can authenticate without user
  intervention.  E.g.  ``ssh user@my-data-host -i
  /home/user/.ssh/unlocked_key``.  This ssh command is used to execute
  a few different commands on the remote system, including ``md5sum``,
  ``du``, and ``tar``.

Special settings:
* ``emulator_dir``: If this setting is present, then the system will
  archive to tar files on the local filesystem at the path indicated
  by ``emulator_dir``.  This can be used to test the system without
  needing to have a real tape drive.


Status and Tape Management
==========================

Show list of tapes
------------------

Run::

  tapeop tapes [-v]

Lists the tapes by their name and serial number, and shows their
status.  This is the default command when no tape is active.  Pass -v
(--verbose) to also print the amount of data that has been written to
the tape, in GB.


Show status of the current tape
-------------------------------

Run::

  tapeop status [tape_name]

Prints a summary of how many targets are archived, confirmed, or
assigned to the current tape, and the total size of each group.  This
is the default command if a tape is active.


Dump description of tape contents
---------------------------------

Run::

  tapeop tape_detail [tape_name] [file_number] [-v]

Prints contents of a tape in tabular format.  Without the -v
(--verbose) option, there is a header line and then one line per
target.  With the -v option, each target description line is followed
by the detail of each file in the target, including size and checksum.
The [file_number] is optional; without it, all targets on the tape
will be printed.


Find a file in the backup archive
---------------------------------

Run::

  tapeop where_is [filename]

Searches the backups (only assigned, archived, or confirmed jobs) for
the indicated filename.  Note that the filename should be bare,
without any path.  Prints out the location and status of all matching
files.


Backup job setup and execution
==============================

Add "targets" to the database, for archiving [import]
-----------------------------------------------------

Run::

  tapeop import FILENAME

The FILENAME is a file that lists the full paths to directories that
need to be backed up.  This job will add those paths to the "targets"
database.  It will also run a remote file find and checksum on each
target.  (This action can thus take several minutes, as all the data
must be loaded from disk on the host machine.)

A good way to generate the path list is with a find command like this
one::

  find /mnt/act6/actpol/data/season4/ -mindepth 2 -maxdepth 2 -type d | \
    grep -v merlin | sort > act6_s4.txt


Assign targets to a particular tape [assign]
--------------------------------------------

Run::

  tapeop assign [TAPE_NAME]

This will associate any unassigned targets to the specified tape (or
the Online tape if TAPE_NAME is not provided).  This effectively
creates a "backup request", which can then be performed with an
"archive" request.


Do a backup [archive]
---------------------

Run::

  tapeop archive

Perform a single archiving action; this amounts to copying the next
"assigned" target to the next open file_number on the active tape.

Run ``tape_batch.bash archive`` to repeatedly perform archive jobs (it
will stop automatically once tape is full or there are no further
targets assigned).


Confirm a backup [confirm]
--------------------------
[status: good]

Run::

  tapeop confirm [id]

where id is the file_number on the tape, or "next" to confirm the next
unconfirmed item.  Options:

* ``--retry``: allows you to re-run confirm on an already-confirmed file.
* ``--no-db-update``: do the confirmation steps but don't change the database.

Run ``tape_batch.bash confirm`` to repeatedly perform confirmation
jobs (it will stop automatically on failure or if there are no
archives left to confirm).


Tape activation / deactivation
==============================

Close a tape [close_tape]
-------------------------

Run::

  tapeop close_tape [tape_name]

Causes the tape to be marked 'closed', taken offline, and all assigned
(but not archived) jobs to be dissociated from the tape.  This leaves
the system in the right state for going to a new tape.  This is
slightly interactive.  But it does NOT check for you that the tape is
full or that you've confirmed all the archives.


Add a new tape and activate it [open_tape]
------------------------------------------

Run::

  tapeop open_tape [tape_name]

The tape_name is not optional!  The script will ask you to confirm;
then ask you for a serial number (which is non-critical meta-data and
can be added/changed later, if need be).  It will also ask you if you
want to put the tape online, meaning that is marked as active.


Activate a tape [activate_tape]
-------------------------------

Run::

  tapeop activate_tape [tape_name]

For some tape that is already registered in the database, this brings
the tape online so that the backups there can be inspected or
extended.


Be Paranoid
===========

How to check "by hand" that everything is working properly?  Once in a
while it's reassuring to double-check that the archiving and
confirmation are doing what we expect.

The session below shows how to extract the archive at file number 100
on the current tape, checksum the result, and compare those checksums
to the database copy::

  # Define my non-rewinding, non-compressing tape device.
  # Commands to mt will refer to the value of $TAPE by default.

  export TAPE=/dev/nst0


  # Rewind the tape, then step forward to file_number... 100.

  mt rewind
  mt fsf 100
  

  # Dump the contents of this tape file to the tar command.  This 
  # will recreate the tree of files in ./ .

  cat $TAPE | tar -x
  
  
  # Generate an md5sums file for this archive, based on values stored
  # in the tapetown database.  Let's assume we're in the directory
  # "~/tapework/" right now.

  ./tapeop tape_detail act-2018-008 100 -v | \
    awk '($1=="file") {printf "%s  %s\n", $2,$4;}' > md5sums.txt

  
  # Descend into the extracted directory, and run md5sum in
  # verification mode.

  cd mnt/act6/actpol/data/season4/mce3/14771/
  md5sum -c ~/tapework/md5sums.txt 


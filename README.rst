==========================
TAPE ARCHIVING OF ACT DATA
==========================

Status and Tape Management
==========================


Show list of tapes
------------------
[status: good]

Run:

  tapeop tapes



Backup Workflow
===============

Once a tape is prepared, one proceeds through the following
steps:  import, assign, archive, confirm.

Note that "import" introduces targets into the database without
associating them with a particular tape.  Even once an association of
a target to a particular tape has been made, it can be changed later
provided that the target was never written to that tape.  So in
practice a workflow is more like:

- import a bunch of files
- open tape1
- assign files to tape1
- archive until tape1 is full (leaves some files outstanding)
- confirm tape1
- close tape1
- open tape2
- re-assign outstanding files to tape2
- archive until tape2 is full or all files are backed up
- confirm
- etc.



Add "targets" to the database, for archiving [import]
-----------------------------------------------------
[status: good]

Run:

  tapeop import FILENAME

The FILENAME is a file that lists the full paths to directories that
need to be backed up.  This job will add those paths to the "targets"
database.  It will also run a remote file find and checksum on each
target.  (This action can thus take several minutes, as all the data
must be loaded from disk on the host machine.)


Assign targets to a particular tape [assign]
--------------------------------------------
[status: good]

Run:

  tapeop assign [TAPE_NAME]

This will associate any unassigned targets to the specified tape (or
the Online tape if TAPE_NAME is not provided).  This effectively
creates a "backup request", which can then be performed with an
"archive" request.


Do a backup [archive]
---------------------
[status: good]

Run:

  tapeop archive

Perform a single archiving action; this amounts to copying the next
"assigned" target to the next open file_number on the active tape.



Confirm a backup [confirm]
--------------------------
[status: good]

Run:

  tapeop confirm [id]

where id is the file_number on the tape, or "next" to confirm the next
unconfirmed item.  Options:

  --retry : allows you to re-run confirm on an already-confirmed file.
  --no-db-update : do the confirmation steps but don't change the database.


Close a tape [close_tape]
-------------------------

Run:

  tapeop close_tape [tape_name]

Causes the tape to be marked 'closed', taken offline, and all assigned
(but not archived) jobs to be dissociated from the tape.  This leaves
the system in the right state for going to a new tape.  This is
slightly interactive.  But it does NOT check for you that the tape is
full or that you've confirmed all the archives.


Add a new tape and activate it [open_tape]
------------------------------------------

Run:

  tapeop open_tape [tape_name]

The tape_name is not optional!  The script will ask you to confirm;
then ask you for a serial number (which is non-critical meta-data and
can be added/changed later, if need be).  It will also ask you if you
want to put the tape online, meaning that is marked as active.

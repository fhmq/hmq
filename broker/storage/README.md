# Storage log guarantees

This package is a single-node append log. Broker retained-message integration
is optional; replication is not implemented.

Linux and macOS are supported. Open takes a non-blocking advisory exclusive
flock on the file before recovery and holds it until Close. Another cooperating
Open, including through a hard link, fails with ErrLocked. Locks do not prevent
non-cooperating writes, unlinking, renaming, or replacement of the file; callers
must prevent those operations and must use a filesystem with working flock
semantics. In particular, network filesystems may have different semantics.

Open syncs the file and its parent directory, including when the file already
exists, and fails if either sync fails. The parent directory must already exist
and its own entry and ancestors must already be durable. Each successful Append
syncs the file. These guarantees
depend on the filesystem, operating system and hardware honoring sync; they do
not protect against media failure or external modification. On macOS ordinary
fsync is used, not F_FULLFSYNC, so no stronger hardware-flush guarantee is made.
Other operating systems return ErrUnsupported without creating the file.

Only the payload has a checksum. An incomplete header or payload is rejected
without truncating the file: a partial write cannot be distinguished from
damage to previously committed data. A complete record with a mismatching
payload checksum is rejected. CRC is not protection against malicious
modification, and corruption that removes entire records cannot be detected.
After a write or sync error the result of that append is uncertain and the log
must be closed and reopened. ReadAll loads the entire log into memory.
Replay validates and delivers one independently owned record at a time, so its
working memory is bounded by MaxRecordSize unless the callback retains data.
It holds the log lock throughout replay: concurrent operations wait, and the
callback must not call methods on the same log. A callback error stops replay
without poisoning the log or undoing previously applied callbacks. A read or
corruption error likewise leaves earlier callback effects intact.

Replace writes a caller-provided recovery snapshot into a private temporary
file, locks and syncs it, renames it over the log, then syncs the actual parent
directory. The new inode remains locked throughout installation. A directory
sync failure after rename poisons the log; close and reopen to determine the
installed state. Failures before rename leave the original log usable.
Replacement rejects logs with multiple hard links. Do not create hard links,
move directories, or externally replace files during a log's lifetime. These
checks protect cooperative callers, not malicious directory writers.

Callers must serialize snapshot creation and Replace with their own business
mutations, and retain identity, sequence high-water marks, deduplication and
pending protocol state. This primitive alone does not enable compaction in
eventstore or sessionstore. Crash leftovers with `.hmq-replace-` names are never
selected automatically as recovery logs. The implementation inherits the
filesystem durability limitations above; SIGKILL tests cannot prove power-loss
durability.

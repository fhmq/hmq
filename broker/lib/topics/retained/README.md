# Retained store

This standalone store is not wired into the broker. Put and Delete return
success only after the underlying append and file sync succeed. A write or
sync error leaves the outcome uncertain; close and reopen before continuing.
Get returns a copy. Empty payloads are stored as messages; callers must use
Delete explicitly. The log grows without compaction and recovery reads it into
memory. Records and payloads are bounded by storage.MaxRecordSize (including
JSON encoding overhead).

The underlying storage package supports Linux and macOS only; other platforms
return storage.ErrUnsupported. Its advisory locking, directory durability,
filesystem and hardware assumptions, corruption detection limits, and macOS
fsync limitations apply here as documented in broker/storage/README.md.

# Optional persistent manager

Use `NewPersistentManager(path)` and close the returned manager when finished.
Subscriptions remain in memory. Retained messages persist topic, QoS and payload;
other publish packet fields are not retained. An empty payload deletes a topic.
This is local persistence, not cluster replication.

Retain, Retained and Close are serialized by one lock. Concurrent operations
take effect in lock acquisition order, not invocation order. A successful
Retain has synced the log and updated the visible tree before returning.
After a storage error, retained reads and writes return that error until the
manager is closed and reopened. A failed write or sync may nevertheless have
reached disk; recovery may include it, or reject an incomplete log. Validation
errors before storage do not poison the manager. Subscription operations are
independent of this ordering.

The storage log's platform, locking, durability and corruption limitations
apply; see `retained/README.md` and `broker/storage/README.md`.

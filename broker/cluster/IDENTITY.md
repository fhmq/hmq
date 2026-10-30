# Cluster certificate identities

Every node must use a distinct certificate whose leaf has exactly one URI SAN:
`urn:hmq:node:<NodeID>`. Node IDs are case-sensitive and consist of 1 to 128
ASCII letters, digits, hyphens or underscores. For example, node `broker-1`
uses `URI:urn:hmq:node:broker-1`. The same identity is required for inbound
and outbound connections; the hello ID must match the URI exactly.

Issue certificates from a dedicated cluster CA and restrict issuance so that
only the owner of a node can obtain its identity. Sharing a private key or
issuing the same identity to multiple nodes defeats this binding. The shared
cluster secret remains necessary but does not grant authority to impersonate
another certificate identity. Standard TLS server hostname verification still
applies, so server certificates also need the appropriate DNS or IP SAN and
both client and server authentication usages. Rotate certificates with the
same URI identity, and remove compromised issuers from trust stores. Existing
certificates without this URI must be replaced before upgrading nodes.

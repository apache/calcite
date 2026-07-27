# Agent guidance

This file is read by automated agents (security scanners, code analyzers,
AI assistants) operating on this repository. It points them at the
human-authored references they should consult before producing output.

## Security

Security model: [SECURITY.md](./SECURITY.md), which links to the project's
threat model at
[site/_docs/security_threat_model.md](./site/_docs/security_threat_model.md).

Calcite is an embedded SQL framework, not a server. It opens no socket and
has no authentication or authorization of its own; the host application
owns transport, identity, and schema scoping. Read the threat model before
reporting anything — it is explicit about what it does and does not treat
as a vulnerability.

Two rules carry most of the triage weight:

- **Surprising vs unsurprising class loading.** A class named through a
  Calcite SPI position — `schemaFactory`, `parserFactory`, `typeSystem`,
  `metaTableFactory`, `metaColumnFactory`, `tableFactory`, function
  classes, `dataSource`, `jdbcDriver`, `model` — is loaded only through
  that SPI, gated by `Class.forName(name, false, loader)` plus an
  `isAssignableFrom` check. A class that does not implement the SPI for
  its position is never instantiated by name. SQL may name SPI classes,
  but only SPI implementations run, and only through their SPI.
- **Pushed-down SQL.** The SQL Calcite generates and sends to a backend
  the operator configured is *not* a vulnerability — the query author can
  already reach that endpoint through the visible schemas. A pushdown bug
  that reads *beyond* the configured schemas is P4 and *is* one.

Explicitly not vulnerabilities (see the model's "Not a vulnerability"
section): the os-adapter running OS commands, the file/CSV/JSON adapters
reading paths they were configured with, anything requiring a changed
system property or classpath, a third-party driver's behaviour past the
connection boundary, and cross-tenant reads that follow from the embedder
exposing several principals' schemas on one connection.

The model also lists what belongs to the host rather than the library —
transport and identity, schema scoping, adapter selection, the classpath,
and whatever a `model` points at — under "Downstream responsibilities".

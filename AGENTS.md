# Agent guidance

Apache Calcite is a dynamic data management framework. It provides a SQL
parser and validator, a customizable cost-based optimizer, and relational
algebra operators, without a storage engine of its own.

## Security

See [SECURITY.md](./SECURITY.md) before reporting a vulnerability.

## Commit messages

Always end commit messages, including drafts, with `Assisted-by: <tool> (<model-id>)`,
never `Co-Authored-By:`. Keep existing trailers and add yours when amending someone
else's commit.

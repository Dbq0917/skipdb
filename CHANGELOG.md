# UNRELEASED

- Compact automatically when tombstones reach half the number of live keys, so
  delete-heavy workloads cannot accumulate an unbounded number of tombstones.

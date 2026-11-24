# UNRELEASED

- Add `snapshot` APIs for `OptimisticDb` and `SerializableDb`. A snapshot pins
  the MVCC version observed by an export without blocking concurrent writes.

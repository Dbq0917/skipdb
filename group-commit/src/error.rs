use std::fmt;
use std::sync::Arc;

/// Error returned to a transaction after it was accepted by the committer.
///
/// The background thread owns the log, so errors must be shareable across all
/// transactions that were riding the same failed batch; hence the [`Arc`].
#[derive(Clone)]
pub enum CommitError<E: fmt::Display + fmt::Debug + Send + Sync + 'static> {
  /// The log append or flush failed. Every transaction in the batch receives
  /// the same error, and none of them is acknowledged as durable.
  Log(Arc<E>),
  /// The committer shut down (or panicked) while the transaction was queued.
  /// Nothing past the last successful flush was acknowledged as durable.
  Closed,
}

impl<E: fmt::Display + fmt::Debug + Send + Sync + 'static> fmt::Debug for CommitError<E> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Log(err) => f.debug_tuple("CommitError::Log").field(err).finish(),
      Self::Closed => write!(f, "CommitError::Closed"),
    }
  }
}

impl<E: fmt::Display + fmt::Debug + Send + Sync + 'static> fmt::Display for CommitError<E> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Log(err) => write!(f, "group commit log flush failed: {err}"),
      Self::Closed => write!(f, "group committer is closed"),
    }
  }
}

impl<E: fmt::Display + fmt::Debug + Send + Sync + 'static> std::error::Error for CommitError<E> {}

/// Error returned when a record could not even be queued, so the caller keeps
/// ownership of its payload and can retry or handle it synchronously.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
  /// The bounded queue is full; the payload is handed back untouched. This is
  /// backpressure, not data loss — no flush has been attempted for it.
  Full(Vec<u8>),
  /// The committer has shut down or hit a fatal log error. The payload is
  /// handed back untouched.
  Closed(Vec<u8>),
}

impl fmt::Display for SubmitError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Full(_) => write!(f, "group commit queue is full"),
      Self::Closed(_) => write!(f, "group committer is closed"),
    }
  }
}

impl std::error::Error for SubmitError {}

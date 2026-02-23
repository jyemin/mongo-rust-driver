use std::convert::TryInto;

use crate::{
    bson::{Document, RawBsonRef, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    client::SESSIONS_UNSUPPORTED_COMMANDS,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    selection_criteria::SelectionCriteria,
    Database,
};

use super::{ExecutionContext, OperationWithDefaults, Retryability};

#[derive(Debug, Clone)]
pub(crate) struct RunCommand<'conn> {
    db: Database,
    command: RawDocumentBuf,
    selection_criteria: Option<SelectionCriteria>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
    /// If true, the driver will not add session info (lsid, txnNumber, etc.) to the command.
    /// Used when the command already contains session info from an external source (e.g., Java FFI).
    skip_session_injection: bool,
}

impl<'conn> RunCommand<'conn> {
    pub(crate) fn new(
        db: Database,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Self {
        Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
            skip_session_injection: false,
        }
    }

    pub(crate) fn new_with_external_session(
        db: Database,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Self {
        Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
            skip_session_injection: true,
        }
    }

    pub(crate) fn set_skip_session_injection(&mut self, skip: bool) {
        self.skip_session_injection = skip;
    }

    fn command_name(&self) -> Option<&CStr> {
        self.command
            .into_iter()
            .next()
            .and_then(|r| r.ok())
            .map(|(k, _)| k)
    }
}

impl OperationWithDefaults for RunCommand<'_> {
    type O = Document;

    // Since we can't actually specify a string statically here, we just put a descriptive string
    // that should fail loudly if accidentally passed to the server.
    const NAME: &'static CStr = cstr!("$genericRunCommand");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        if self.command_name().is_none() {
            return Err(Error::invalid_argument(
                "an empty document cannot be passed to a run_command operation",
            ));
        }

        Ok(Command::from_operation(self, self.command.clone()))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        if let Some(RawBsonRef::Timestamp(ts)) = response.get("atClusterTime")? {
            Ok(Some(ts))
        } else {
            super::cursor_get_at_cluster_time(response)
        }
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(response.raw_body().try_into()?)
    }

    fn selection_criteria(&self) -> super::Feature<&SelectionCriteria> {
        // Per spec, runCommand MUST ignore any default read preference from client, database or
        // collection configuration
        match &self.selection_criteria {
            Some(s) => super::Feature::Set(s),
            None => super::Feature::NotSupported,
        }
    }

    fn supports_sessions(&self) -> bool {
        // If skip_session_injection is set, return false to prevent the driver from
        // adding lsid/txnNumber/etc. (the command already has them from external source)
        if self.skip_session_injection {
            return false;
        }
        self.command_name()
            .map(|command_name| {
                !SESSIONS_UNSUPPORTED_COMMANDS.contains(command_name.to_lowercase().as_str())
            })
            .unwrap_or(false)
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }

    fn target(&self) -> super::OperationTarget {
        (&self.db).into()
    }

    fn name(&self) -> &CStr {
        self.command_name().unwrap_or(Self::NAME)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for RunCommand<'_> {}

/// A variant of RunCommand that returns RawDocumentBuf instead of Document.
/// This avoids the cost of parsing the response into a Document.
#[derive(Debug, Clone)]
pub(crate) struct RunCommandRaw<'conn> {
    db: Database,
    command: RawDocumentBuf,
    selection_criteria: Option<SelectionCriteria>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
    retryability: Retryability,
    /// If true, the driver will not add session info (lsid, txnNumber, etc.) to the command.
    /// Used when the command already contains session info from an external source (e.g., Java FFI).
    skip_session_injection: bool,
}

impl<'conn> RunCommandRaw<'conn> {
    pub(crate) fn new(
        db: Database,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Self {
        Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
            retryability: Retryability::None,
            skip_session_injection: false,
        }
    }

    /// Create a new RunCommandRaw with specified retryability
    pub(crate) fn new_with_retryability(
        db: Database,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
        retryability: Retryability,
    ) -> Self {
        Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
            retryability,
            skip_session_injection: false,
        }
    }

    /// Create a new RunCommandRaw with specified retryability and session skip flag
    pub(crate) fn new_with_external_session(
        db: Database,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
        retryability: Retryability,
    ) -> Self {
        Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
            retryability,
            skip_session_injection: true,
        }
    }

    fn command_name(&self) -> Option<&CStr> {
        self.command
            .into_iter()
            .next()
            .and_then(|r| r.ok())
            .map(|(k, _)| k)
    }
}

impl OperationWithDefaults for RunCommandRaw<'_> {
    type O = RawDocumentBuf;

    const NAME: &'static CStr = cstr!("$genericRunCommandRaw");

    // Enable zero-copy to receive Cow::Owned in handle_response_cow
    const ZERO_COPY: bool = true;

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        if self.command_name().is_none() {
            return Err(Error::invalid_argument(
                "an empty document cannot be passed to a run_command operation",
            ));
        }

        Ok(Command::from_operation(self, self.command.clone()))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        if let Some(RawBsonRef::Timestamp(ts)) = response.get("atClusterTime")? {
            Ok(Some(ts))
        } else {
            super::cursor_get_at_cluster_time(response)
        }
    }

    // Override handle_response_cow to take ownership of the response (zero-copy)
    fn handle_response_cow<'a>(
        &'a self,
        response: std::borrow::Cow<'a, RawCommandResponse>,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        // Take ownership and extract RawDocumentBuf without copying
        Ok(response.into_owned().into_raw_document_buf())
    }

    fn selection_criteria(&self) -> super::Feature<&SelectionCriteria> {
        match &self.selection_criteria {
            Some(s) => super::Feature::Set(s),
            None => super::Feature::NotSupported,
        }
    }

    fn supports_sessions(&self) -> bool {
        // If skip_session_injection is set, return false to prevent the driver from
        // adding lsid/txnNumber/etc. (the command already has them from external source)
        if self.skip_session_injection {
            return false;
        }
        self.command_name()
            .map(|command_name| {
                !SESSIONS_UNSUPPORTED_COMMANDS.contains(command_name.to_lowercase().as_str())
            })
            .unwrap_or(false)
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }

    fn target(&self) -> super::OperationTarget {
        (&self.db).into()
    }

    fn name(&self) -> &CStr {
        self.command_name().unwrap_or(Self::NAME)
    }

    fn retryability(&self) -> Retryability {
        self.retryability
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for RunCommandRaw<'_> {}

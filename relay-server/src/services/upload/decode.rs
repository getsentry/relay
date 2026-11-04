use std::io;

/// Marks failures of the encoded input so they are not mistaken for decoder corruption.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(super) struct InputError(io::Error);

impl InputError {
    pub(super) fn wrap(error: io::Error) -> io::Error {
        io::Error::new(error.kind(), Self(error))
    }
}

/// Invalid compressed data, including checksum failures.
#[derive(Debug, thiserror::Error)]
#[error("invalid compressed upload: {0}")]
pub struct DecodeError(#[source] io::Error);

impl DecodeError {
    pub(super) fn wrap(error: io::Error) -> io::Error {
        if error
            .get_ref()
            .is_some_and(|error| error.is::<InputError>())
        {
            return error;
        }
        io::Error::new(error.kind(), Self(error))
    }
}

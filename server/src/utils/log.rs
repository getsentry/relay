use std::fmt;

use failure::Fail;

pub struct LogError<'a, E: Fail>(pub &'a E);

impl<'a, E: Fail> fmt::Display for LogError<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)?;
        for cause in (self.0 as &Fail).iter_causes() {
            write!(f, "\n  caused by: {}", cause)?;
        }

        if log::log_enabled!(log::Level::Debug) {
            if let Some(backtrace) = self.0.backtrace() {
                write!(f, "\n\n{:?}", backtrace)?;
            }
        }

        Ok(())
    }
}

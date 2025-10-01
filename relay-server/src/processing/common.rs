use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::processing::check_ins::CheckInsProcessor;
use crate::processing::logs::LogsProcessor;
use crate::processing::sessions::SessionsProcessor;
use crate::processing::spans::SpansProcessor;
use crate::processing::{Forward, Processor};

macro_rules! outputs {
    ($($variant:ident => $ty:ty,)*) => {
        /// All known [`Processor`] outputs.
        #[derive(Debug)]
        pub enum Outputs {
            $(
                $variant(<$ty as Processor>::Output)
            ),*
        }

        impl Forward for Outputs {
            fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
                match self {
                    $(
                        Self::$variant(output) => output.serialize_envelope()
                    ),*
                }
            }

            #[cfg(feature = "processing")]
            fn forward_store(
                self,
                s: &relay_system::Addr<crate::services::store::Store>,
            ) -> Result<(), Rejected<()>> {
                match self {
                    $(
                        Self::$variant(output) => output.forward_store(s)
                    ),*
                }
            }
        }

        $(
            impl From<<$ty as Processor>::Output> for Outputs {
                fn from(value: <$ty as Processor>::Output) -> Self {
                    Self::$variant(value)
                }
            }
        )*
    };
}

outputs!(
    CheckIns => CheckInsProcessor,
    Logs => LogsProcessor,
    Spans => SpansProcessor,
    Sessions => SessionsProcessor,
);

use std::fmt::Debug;

use chrono::{DateTime, Utc};

use crate::processor::ProcessingState;
use crate::types::{Annotated, Array, FromValue, Object, ToValue, Value};

macro_rules! process_method {
    ($name:ident, $ty:ty) => {
        process_method!($name, $ty, stringify!($ty));
    };
    ($name:ident, $ty:ty, $help_ty:expr) => {
        #[doc = "Processes values of type `"]
        #[doc = $help_ty]
        #[doc = "`."]
        fn $name(&mut self, value: Annotated<$ty>, state: ProcessingState) -> Annotated<$ty>
            where Self: Sized
        {
            ProcessValue::process_child_values(value, self, state)
        }
    }
}

/// A trait for processing the protocol.
pub trait Processor {
    // primitives
    process_method!(process_string, String);
    process_method!(process_u64, u64);
    process_method!(process_i64, i64);
    process_method!(process_f64, f64);
    process_method!(process_bool, bool);
    process_method!(process_datetime, DateTime<Utc>);

    // values and databags
    process_method!(process_value, Value);

    fn process_array<T: ProcessValue>(
        &mut self,
        value: Annotated<Array<T>>,
        state: ProcessingState,
    ) -> Annotated<Array<T>>
    where
        Self: Sized,
    {
        ProcessValue::process_child_values(value, self, state)
    }
    fn process_object<T: ProcessValue>(
        &mut self,
        value: Annotated<Object<T>>,
        state: ProcessingState,
    ) -> Annotated<Object<T>>
    where
        Self: Sized,
    {
        ProcessValue::process_child_values(value, self, state)
    }

    fn process_values<T: ProcessValue>(
        &mut self,
        value: Annotated<crate::protocol::Values<T>>,
        state: ProcessingState,
    ) -> Annotated<crate::protocol::Values<T>>
    where
        Self: Sized,
    {
        ProcessValue::process_child_values(value, self, state)
    }

    // interfaces
    process_method!(process_event, crate::protocol::Event);
    process_method!(process_exception, crate::protocol::Exception);
    process_method!(process_stacktrace, crate::protocol::Stacktrace);
    process_method!(process_frame, crate::protocol::Frame);
    process_method!(process_request, crate::protocol::Request);
    process_method!(process_user, crate::protocol::User);
    process_method!(process_client_sdk_info, crate::protocol::ClientSdkInfo);
    process_method!(process_debug_meta, crate::protocol::DebugMeta);
    process_method!(process_geo, crate::protocol::Geo);
    process_method!(process_logentry, crate::protocol::LogEntry);
    process_method!(process_thread, crate::protocol::Thread);
    process_method!(process_context, crate::protocol::Context);
    process_method!(process_breadcrumb, crate::protocol::Breadcrumb);
    process_method!(process_template_info, crate::protocol::TemplateInfo);
}

/// Implemented for all processable meta structures.
///
/// The intended behavior is that implementors make `process_value` call
/// into a fallback on a `Processor` and that this processor by default
/// calls into `process_child_values`.  The default behavior that is
/// implemented is to make `process_value` directly call into
/// `process_child_values`.
pub trait ProcessValue: FromValue + ToValue + Debug {
    /// Executes a processor on the tree.
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        ProcessValue::process_child_values(value, processor, state)
    }

    /// Only processes the child values.
    fn process_child_values<P: Processor>(
        value: Annotated<Self>,
        processor: &mut P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        let _processor = processor;
        let _state = state;
        value
    }
}

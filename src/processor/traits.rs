use chrono::{DateTime, Utc};

use super::*;
use crate::types::*;

/// Implemented for all meta structures.
pub trait FromValue {
    /// Creates a meta structure from an annotated boxed value.
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;
}

/// Implemented for all meta structures.
pub trait ToValue {
    /// Boxes the meta structure back into a value.
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized;

    /// Extracts children meta map out of a value.
    #[inline(always)]
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Efficiently serializes the payload directly.
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer;

    /// Extracts the meta tree out of annotated value.
    ///
    /// This should not be overridden by implementators, instead `extract_child_meta`
    /// should be provided instead.
    #[inline(always)]
    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        MetaTree {
            meta: value.1.clone(),
            children: match value.0 {
                Some(ref value) => ToValue::extract_child_meta(value),
                None => Default::default(),
            },
        }
    }
}

macro_rules! process_method {
    ($name:ident, $ty:ty) => {
        process_method!($name, $ty, stringify!($ty));
    };
    ($name:ident, $ty:ty, $help_ty:expr) => {
        #[inline(always)]
        #[doc = "Processes values of type `"]
        #[doc = $help_ty]
        #[doc = "`."]
        fn $name(&self, value: Annotated<$ty>, state: ProcessingState) -> Annotated<$ty>
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

    #[inline(always)]
    fn process_array<T: ProcessValue>(
        &self,
        value: Annotated<Array<T>>,
        state: ProcessingState,
    ) -> Annotated<Array<T>>
    where
        Self: Sized,
    {
        ProcessValue::process_child_values(value, self, state)
    }
    #[inline(always)]
    fn process_object<T: ProcessValue>(
        &self,
        value: Annotated<Object<T>>,
        state: ProcessingState,
    ) -> Annotated<Object<T>>
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

pub trait ProcessValue {
    /// Executes a processor on the tree.
    #[inline(always)]
    fn process_value<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self>
    where
        Self: Sized,
    {
        ProcessValue::process_child_values(value, processor, state)
    }

    #[inline(always)]
    fn process_child_values<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
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

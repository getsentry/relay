// This module only defines traits, every parameter is used by definition
#![allow(unused_variables)]

use std::fmt::Debug;

use crate::processor::ProcessingState;
use crate::types::{FromValue, Meta, ToValue, ValueAction};

macro_rules! process_method {
    ($name: ident, $ty:ident $(::$path:ident)*) => {
        #[inline]
        fn $name(
            &mut self,
            value: &mut $ty $(::$path)*,
            meta: &mut Meta,
            state: ProcessingState<'_>,
        ) -> ValueAction {
            value.process_child_values(self, state);
            Default::default()
        }
    };

    ($name: ident, $ty:ident $(::$path:ident)* < $($param:ident),+ >) => {
        #[inline]
        fn $name<$($param),*>(
            &mut self,
            value: &mut $ty $(::$path)* <$($param),*>,
            meta: &mut Meta,
            state: ProcessingState<'_>,
        ) -> ValueAction
        where
            $($param: ProcessValue),*
        {
            value.process_child_values(self, state);
            Default::default()
        }
    };
}

/// A trait for processing processable values.
pub trait Processor: Sized {
    process_method!(process_string, String);
    process_method!(process_u64, u64);
    process_method!(process_i64, i64);
    process_method!(process_f64, f64);
    process_method!(process_bool, bool);

    process_method!(process_value, crate::types::Value);
    process_method!(process_array, crate::types::Array<T>);
    process_method!(process_object, crate::types::Object<T>);

    process_method!(process_pairlist, crate::protocol::PairList<T>);
    process_method!(process_values, crate::protocol::Values<T>);

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

/// A recursively processable value.
pub trait ProcessValue: FromValue + ToValue + Debug {
    /// Executes a processor on this value.
    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: ProcessingState<'_>,
    ) -> ValueAction
    where
        P: Processor,
    {
        self.process_child_values(processor, state);
        Default::default()
    }

    /// Recurses into children of this value.
    #[inline]
    fn process_child_values<P>(&mut self, processor: &mut P, state: ProcessingState<'_>)
    where
        P: Processor,
    {
    }
}

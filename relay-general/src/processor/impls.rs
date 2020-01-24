use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::processor::{
    process_value, AttrMap, Attributes, ProcessValue, ProcessingState, Processor, ValueType,
};
use crate::types::{Annotated, Array, Meta, Object, ProcessingResult};

impl ProcessValue for String {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::String)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_string(self, meta, state)
    }
}

impl ProcessValue for bool {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Boolean)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_bool(self, meta, state)
    }
}

impl ProcessValue for u64 {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Number)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_u64(self, meta, state)
    }
}

impl ProcessValue for i64 {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Number)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_i64(self, meta, state)
    }
}

impl ProcessValue for f64 {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Number)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_f64(self, meta, state)
    }
}

impl ProcessValue for DateTime<Utc> {
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::DateTime)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_timestamp(self, meta, state)
    }
}

impl ProcessValue for Uuid {}

impl<T> ProcessValue for Array<T>
where
    T: ProcessValue,
{
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Array)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_array(self, meta, state)
    }

    #[inline]
    fn process_child_values<P>(
        &mut self,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        for (index, element) in self.iter_mut().enumerate() {
            process_value(
                element,
                processor,
                &state.enter_index(index, None, ValueType::for_field(element)),
            )?;
        }

        Ok(())
    }
}

impl<T> ProcessValue for Object<T>
where
    T: ProcessValue,
{
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Object)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        processor.process_object(self, meta, state)
    }

    #[inline]
    fn process_child_values<P>(
        &mut self,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        for (k, v) in self.iter_mut() {
            process_value(
                v,
                processor,
                &state.enter_borrowed(k, None, ValueType::for_field(v)),
            )?;
        }

        Ok(())
    }
}

impl<T> ProcessValue for Box<T>
where
    T: ProcessValue,
{
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        (**self).value_type()
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        P: Processor,
    {
        ProcessValue::process_value(self.as_mut(), meta, processor, state)
    }
}

macro_rules! process_tuple {
    ($($name: ident),+) => {
        impl< $( $name: ProcessValue ),* > ProcessValue for ( $( Annotated<$name>, )* ) {
            #[inline]
            fn value_type(&self) -> Option<ValueType> {
                Some(ValueType::Array)
            }

            #[inline]
            #[allow(non_snake_case, unused_assignments)]
            fn process_child_values<P>(&mut self, processor: &mut P, state: &ProcessingState<'_>)
                -> ProcessingResult
            where
                P: Processor,
            {
                let ($(ref mut $name,)*) = *self;
                let mut index = 0;

                $(
                    process_value($name, processor, &state.enter_index(index, None, ValueType::for_field($name)))?;
                    index += 1;
                )*

                Ok(())
            }
        }
    };
}

process_tuple!(T1);
process_tuple!(T1, T2);
process_tuple!(T1, T2, T3);
process_tuple!(T1, T2, T3, T4);
process_tuple!(T1, T2, T3, T4, T5);
process_tuple!(T1, T2, T3, T4, T5, T6);
process_tuple!(T1, T2, T3, T4, T5, T6, T7);
process_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
process_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
process_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
process_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
process_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

impl<T: AttrMap> Attributes<T> for bool {}
impl<T: AttrMap> Attributes<T> for DateTime<Utc> {}
impl<T: AttrMap> Attributes<T> for String {}
impl<T: AttrMap> Attributes<T> for u64 {}
impl<T: AttrMap> Attributes<T> for i64 {}
impl<T: AttrMap> Attributes<T> for f64 {}
impl<T: AttrMap> Attributes<T> for Uuid {}
impl<T: AttrMap, U> Attributes<T> for Vec<U> {}
impl<T: AttrMap, K, V> Attributes<T> for std::collections::BTreeMap<K, V> {}

impl<T: AttrMap, U: Attributes<T>> Attributes<T> for Box<U> {
    fn get_attrs(&self) -> T {
        (**self).get_attrs()
    }
}

macro_rules! tuple_schema_validated {
    () => {
        impl <T: AttrMap> Attributes<T> for () {}
    };
    ($final_name:ident $(, $name:ident)*) => {
        impl <T: AttrMap, $final_name $(, $name)*> Attributes<T> for ($final_name $(, $name)*, ) {}
        tuple_schema_validated!($($name),*);
    };
}

tuple_schema_validated!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

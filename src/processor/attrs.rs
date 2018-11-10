use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::fmt;
use std::rc::Rc;

/// The maximum size of a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum CapSize {
    EnumLike,
    Summary,
    Message,
    SmallPayload,
    Payload,
    Symbol,
    Path,
    ShortPath,
    Email,
    Culprit,
    TagKey,
    TagValue,
    Hard(usize),
    Soft(usize),
}

impl CapSize {
    pub fn max_chars(self) -> usize {
        match self {
            CapSize::EnumLike => 128,
            CapSize::Summary => 1024,
            CapSize::Message => 8196,
            CapSize::SmallPayload => 4096,
            CapSize::Payload => 20_000,
            CapSize::Symbol => 256,
            CapSize::Path => 256,
            CapSize::ShortPath => 128,
            // these are from constants.py
            CapSize::Email => 75,
            CapSize::Culprit => 200,
            CapSize::TagKey => 32,
            CapSize::TagValue => 200,
            CapSize::Soft(len) | CapSize::Hard(len) => len,
        }
    }

    pub fn grace_chars(self) -> usize {
        match self {
            CapSize::EnumLike => 0,
            CapSize::Summary => 100,
            CapSize::Message => 200,
            CapSize::SmallPayload => 128,
            CapSize::Payload => 1000,
            CapSize::Symbol => 20,
            CapSize::Path => 40,
            CapSize::ShortPath => 20,
            CapSize::Email => 0,
            CapSize::Culprit => 0,
            CapSize::TagKey => 0,
            CapSize::TagValue => 0,
            CapSize::Soft(_) => 10,
            CapSize::Hard(_) => 0,
        }
    }

    pub fn depth(&self) -> usize {
        match *self {
            CapSize::EnumLike => 1,
            CapSize::Summary => 1,
            CapSize::Message => 1,
            CapSize::SmallPayload => 3,
            CapSize::Payload => 5,
            CapSize::Symbol => 1,
            CapSize::Path => 1,
            CapSize::ShortPath => 1,
            CapSize::Email => 1,
            CapSize::Culprit => 1,
            CapSize::TagKey => 1,
            CapSize::TagValue => 1,
            CapSize::Soft(depth) => depth,
            CapSize::Hard(depth) => depth,
        }
    }
}

/// The type of PII contained on a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum PiiKind {
    Freeform,
    Ip,
    Id,
    Username,
    Hostname,
    Sensitive,
    Name,
    Email,
    Location,
    Databag,
}

/// Meta information about a field.
#[derive(Debug, Clone)]
pub struct FieldAttrs {
    /// Optionally the name of the field.
    pub name: Option<&'static str>,
    /// If the field is required.
    pub required: bool,
    /// The maximum size of the field.
    pub cap_size: Option<CapSize>,
    /// The type of PII on the field.
    pub pii_kind: Option<PiiKind>,
}

const DEFAULT_FIELD_ATTRS: FieldAttrs = FieldAttrs {
    name: None,
    required: false,
    cap_size: None,
    pii_kind: None,
};

impl Default for FieldAttrs {
    fn default() -> FieldAttrs {
        DEFAULT_FIELD_ATTRS.clone()
    }
}

#[derive(Debug, Clone)]
enum PathItem<'a> {
    StaticKey(&'a str),
    DynamicKey(String),
    Index(usize),
}

impl<'a> fmt::Display for PathItem<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PathItem::StaticKey(s) => f.pad(s),
            PathItem::DynamicKey(ref s) => f.pad(s.as_str()),
            PathItem::Index(val) => write!(f, "{}", val),
        }
    }
}

/// Processing state passed downwards during processing.
#[derive(Debug, Clone)]
pub struct ProcessingState<'a> {
    parent: Option<&'a ProcessingState<'a>>,
    path: Option<PathItem<'a>>,
    attrs: Option<Cow<'static, FieldAttrs>>,
    ext: Option<Rc<Vec<(TypeId, Rc<Box<Any>>)>>>,
}

impl<'a> ProcessingState<'a> {
    /// Returns the root processing state.
    pub fn root() -> ProcessingState<'a> {
        ProcessingState {
            parent: None,
            path: None,
            attrs: None,
            ext: None,
        }
    }

    /// Derives a processing state by entering a static key.
    #[inline(always)]
    pub fn enter_static(
        &'a self,
        key: &'static str,
        attrs: Option<Cow<'static, FieldAttrs>>,
    ) -> ProcessingState<'a> {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::StaticKey(key)),
            attrs,
            ext: self.ext.clone(),
        }
    }

    /// Derives a processing state by entering a borrowed key.
    #[inline(always)]
    pub fn enter_borrowed(
        &'a self,
        key: &'a str,
        attrs: Option<Cow<'static, FieldAttrs>>,
    ) -> ProcessingState<'a> {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::StaticKey(key)),
            attrs,
            ext: self.ext.clone(),
        }
    }

    /// Derives a processing state by entering an index.
    #[inline(always)]
    pub fn enter_index(
        &'a self,
        idx: usize,
        attrs: Option<Cow<'static, FieldAttrs>>,
    ) -> ProcessingState<'a> {
        ProcessingState {
            parent: Some(self),
            path: Some(PathItem::Index(idx)),
            attrs,
            ext: self.ext.clone(),
        }
    }

    /// Returns the path in the processing state.
    #[inline(always)]
    pub fn path(&'a self) -> Path<'a> {
        Path(&self)
    }

    /// Returns the field attributes.
    #[inline(always)]
    pub fn attrs(&self) -> &FieldAttrs {
        match self.attrs {
            Some(ref cow) => &cow,
            None => &DEFAULT_FIELD_ATTRS,
        }
    }

    /// Get a value from the state.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        if let Some(ref map) = self.ext {
            for &(type_id, ref boxed_rc) in map.iter() {
                if type_id == TypeId::of::<T>() {
                    return (&***boxed_rc as &(Any + 'static)).downcast_ref();
                }
            }
        }
        None
    }
    /// Get a value from the state or insert the default.
    pub fn get_or_default<T: Default + 'static>(&mut self) -> &T {
        if self.get::<T>().is_none() {
            self.set(T::default());
        }
        self.get().unwrap()
    }
    /// Sets a value to the state.
    pub fn set<T: 'static>(&mut self, val: T) {
        self.ext = Some(Rc::new(
            self.ext
                .as_ref()
                .map_or(&[][..], |x| &x[..])
                .iter()
                .filter_map(|&(type_id, ref boxed_rc)| {
                    if type_id == TypeId::of::<T>() {
                        None
                    } else {
                        Some((type_id, boxed_rc.clone()))
                    }
                }).chain(Some((TypeId::of::<T>(), Rc::new(Box::new(val) as Box<Any>))).into_iter())
                .collect(),
        ));
    }
}

/// Represents the path in a structure
#[derive(Debug)]
pub struct Path<'a>(&'a ProcessingState<'a>);

impl<'a> Path<'a> {
    /// Returns the current key if there is one
    #[inline(always)]
    pub fn key(&self) -> Option<&str> {
        self.0.path.as_ref().and_then(|value| match *value {
            PathItem::StaticKey(s) => Some(s),
            PathItem::DynamicKey(ref s) => Some(s.as_str()),
            PathItem::Index(_) => None,
        })
    }

    /// Returns the current index if there is one
    #[inline(always)]
    pub fn index(&self) -> Option<usize> {
        self.0.path.as_ref().and_then(|value| match *value {
            PathItem::StaticKey(_) => None,
            PathItem::DynamicKey(_) => None,
            PathItem::Index(idx) => Some(idx),
        })
    }

    /// Returns a path iterator.
    pub fn iter(&'a self) -> impl Iterator<Item = &'a PathItem<'a>> {
        let mut items = vec![];
        let mut ptr = Some(self.0);
        while let Some(p) = ptr {
            if let Some(ref path) = p.path {
                items.push(path);
            }
            ptr = p.parent;
        }
        items.reverse();
        items.into_iter()
    }
}

impl<'a> fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let path = self.0.path();
        for (idx, item) in path.iter().enumerate() {
            if idx > 0 {
                write!(f, ".")?;
            }
            write!(f, "{}", item)?;
        }
        Ok(())
    }
}

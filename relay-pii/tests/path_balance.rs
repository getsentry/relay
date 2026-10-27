//! Probes that the path stack of `transform::Deserializer` stays balanced.

use std::cell::RefCell;
use std::rc::Rc;

use relay_pii::transform::{Deserializer, Transform};

/// Records every push/pop so tests can compare them, independent of any debug assertion.
#[derive(Default, Clone)]
struct Trace(Rc<RefCell<Vec<String>>>);

impl<'de> Transform<'de> for Trace {
    fn push_path(&mut self, key: &str) {
        self.0.borrow_mut().push(format!("push({key})"));
    }

    fn pop_path(&mut self) {
        self.0.borrow_mut().push("pop".to_owned());
    }
}

fn trace(json: &str) -> Vec<String> {
    let trace = Trace::default();
    let mut de = serde_json::Deserializer::from_str(json);
    let scrubber = Deserializer::new(&mut de, trace.clone());
    serde_transcode::transcode(scrubber, &mut serde_json::Serializer::new(Vec::new())).unwrap();
    let events = trace.0.borrow().clone();
    events
}

/// `{"n<unicode escape for e>sted": {"k": "x"}}`, with the backslash spliced in at runtime.
fn escaped_unicode_key() -> String {
    format!(r#"{{"n{bs}u0065sted":{{"k":"x"}}}}"#, bs = '\\')
}

fn pushes_and_pops(trace: &[String]) -> (usize, usize) {
    let pushes = trace.iter().filter(|e| e.starts_with("push")).count();
    (pushes, trace.len() - pushes)
}

#[test]
fn keys_with_escapes_keep_push_and_pop_balanced() {
    for json in [
        escaped_unicode_key().as_str(),
        r#"{"a\nb":"x"}"#,
        r#"{"a":{"b\\c":"x","d":"y"}}"#,
        r#"{"a\/b":"x"}"#,
        r#"{"a\nb":{"x":1},"ident":"y"}"#,
    ] {
        let t = trace(json);
        let (pushes, pops) = pushes_and_pops(&t);
        assert_eq!(pushes, pops, "unbalanced for {json}: {t:?}");
    }
}

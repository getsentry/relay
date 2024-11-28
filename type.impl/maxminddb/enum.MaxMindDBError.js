(function() {
    var type_impls = Object.fromEntries([["relay_event_normalization",[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Debug-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-Debug-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> for MaxMindDBError</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Debug.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, f: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.82.0/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.unit.html\">()</a>, <a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/struct.Error.html\" title=\"struct core::fmt::Error\">Error</a>&gt;</h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Debug.html#tymethod.fmt\">Read more</a></div></details></div></details>","Debug","relay_event_normalization::geo::GeoIpError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Display-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-Display-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Display.html\" title=\"trait core::fmt::Display\">Display</a> for MaxMindDBError</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Display.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, fmt: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.82.0/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.unit.html\">()</a>, <a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/struct.Error.html\" title=\"struct core::fmt::Error\">Error</a>&gt;</h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Display.html#tymethod.fmt\">Read more</a></div></details></div></details>","Display","relay_event_normalization::geo::GeoIpError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Error-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-Error-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for MaxMindDBError</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.source\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.30.0\">1.30.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.82.0/src/core/error.rs.html#81\">source</a></span><a href=\"#method.source\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.source\" class=\"fn\">source</a>(&amp;self) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.82.0/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;&amp;(dyn <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> + 'static)&gt;</h4></section></summary><div class='docblock'>Returns the lower-level source of this error, if any. <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.source\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.description\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.82.0/src/core/error.rs.html#107\">source</a></span><a href=\"#method.description\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.description\" class=\"fn\">description</a>(&amp;self) -&gt; &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a></h4></section></summary><span class=\"item-info\"><div class=\"stab deprecated\"><span class=\"emoji\">👎</span><span>Deprecated since 1.42.0: use the Display impl or to_string()</span></div></span><div class='docblock'> <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.description\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.cause\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.82.0/src/core/error.rs.html#117\">source</a></span><a href=\"#method.cause\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.cause\" class=\"fn\">cause</a>(&amp;self) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.82.0/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;&amp;dyn <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>&gt;</h4></section></summary><span class=\"item-info\"><div class=\"stab deprecated\"><span class=\"emoji\">👎</span><span>Deprecated since 1.33.0: replaced by Error::source, which can support downcasting</span></div></span></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.provide\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://doc.rust-lang.org/1.82.0/src/core/error.rs.html#180\">source</a><a href=\"#method.provide\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.provide\" class=\"fn\">provide</a>&lt;'a&gt;(&amp;'a self, request: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/core/error/struct.Request.html\" title=\"struct core::error::Request\">Request</a>&lt;'a&gt;)</h4></section></summary><span class=\"item-info\"><div class=\"stab unstable\"><span class=\"emoji\">🔬</span><span>This is a nightly-only experimental API. (<code>error_generic_member_access</code>)</span></div></span><div class='docblock'>Provides type-based access to context intended for error reports. <a href=\"https://doc.rust-lang.org/1.82.0/core/error/trait.Error.html#method.provide\">Read more</a></div></details></div></details>","Error","relay_event_normalization::geo::GeoIpError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Error-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-Error-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html\" title=\"trait serde::de::Error\">Error</a> for MaxMindDBError</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.custom\" class=\"method trait-impl\"><a href=\"#method.custom\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#tymethod.custom\" class=\"fn\">custom</a>&lt;T&gt;(msg: T) -&gt; MaxMindDBError<div class=\"where\">where\n    T: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/fmt/trait.Display.html\" title=\"trait core::fmt::Display\">Display</a>,</div></h4></section></summary><div class='docblock'>Raised when there is general error when deserializing a type. <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#tymethod.custom\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.invalid_type\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.invalid_type\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.invalid_type\" class=\"fn\">invalid_type</a>(unexp: <a class=\"enum\" href=\"https://docs.rs/serde/1.0.209/serde/de/enum.Unexpected.html\" title=\"enum serde::de::Unexpected\">Unexpected</a>&lt;'_&gt;, exp: &amp;dyn <a class=\"trait\" href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Expected.html\" title=\"trait serde::de::Expected\">Expected</a>) -&gt; Self</h4></section></summary><div class='docblock'>Raised when a <code>Deserialize</code> receives a type different from what it was\nexpecting. <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.invalid_type\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.invalid_value\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.invalid_value\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.invalid_value\" class=\"fn\">invalid_value</a>(unexp: <a class=\"enum\" href=\"https://docs.rs/serde/1.0.209/serde/de/enum.Unexpected.html\" title=\"enum serde::de::Unexpected\">Unexpected</a>&lt;'_&gt;, exp: &amp;dyn <a class=\"trait\" href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Expected.html\" title=\"trait serde::de::Expected\">Expected</a>) -&gt; Self</h4></section></summary><div class='docblock'>Raised when a <code>Deserialize</code> receives a value of the right type but that\nis wrong for some other reason. <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.invalid_value\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.invalid_length\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.invalid_length\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.invalid_length\" class=\"fn\">invalid_length</a>(len: <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.usize.html\">usize</a>, exp: &amp;dyn <a class=\"trait\" href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Expected.html\" title=\"trait serde::de::Expected\">Expected</a>) -&gt; Self</h4></section></summary><div class='docblock'>Raised when deserializing a sequence or map and the input data contains\ntoo many or too few elements. <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.invalid_length\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.unknown_variant\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.unknown_variant\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.unknown_variant\" class=\"fn\">unknown_variant</a>(variant: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a>, expected: &amp;'static [&amp;'static <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a>]) -&gt; Self</h4></section></summary><div class='docblock'>Raised when a <code>Deserialize</code> enum type received a variant with an\nunrecognized name.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.unknown_field\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.unknown_field\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.unknown_field\" class=\"fn\">unknown_field</a>(field: &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a>, expected: &amp;'static [&amp;'static <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a>]) -&gt; Self</h4></section></summary><div class='docblock'>Raised when a <code>Deserialize</code> struct type received a field with an\nunrecognized name.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.missing_field\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.missing_field\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.missing_field\" class=\"fn\">missing_field</a>(field: &amp;'static <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a>) -&gt; Self</h4></section></summary><div class='docblock'>Raised when a <code>Deserialize</code> struct type expected to receive a required\nfield with a particular name but that field was not present in the\ninput.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.duplicate_field\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://docs.rs/serde/1.0.209/src/serde/de/mod.rs.html#301\">source</a><a href=\"#method.duplicate_field\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://docs.rs/serde/1.0.209/serde/de/trait.Error.html#method.duplicate_field\" class=\"fn\">duplicate_field</a>(field: &amp;'static <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.str.html\">str</a>) -&gt; Self</h4></section></summary><div class='docblock'>Raised when a <code>Deserialize</code> struct type received more than one of the\nsame field.</div></details></div></details>","Error","relay_event_normalization::geo::GeoIpError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-From%3CError%3E-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-From%3CError%3E-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/convert/trait.From.html\" title=\"trait core::convert::From\">From</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/std/io/error/struct.Error.html\" title=\"struct std::io::error::Error\">Error</a>&gt; for MaxMindDBError</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.from\" class=\"method trait-impl\"><a href=\"#method.from\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/convert/trait.From.html#tymethod.from\" class=\"fn\">from</a>(err: <a class=\"struct\" href=\"https://doc.rust-lang.org/1.82.0/std/io/error/struct.Error.html\" title=\"struct std::io::error::Error\">Error</a>) -&gt; MaxMindDBError</h4></section></summary><div class='docblock'>Converts to this type from the input type.</div></details></div></details>","From<Error>","relay_event_normalization::geo::GeoIpError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-PartialEq-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-PartialEq-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/cmp/trait.PartialEq.html\" title=\"trait core::cmp::PartialEq\">PartialEq</a> for MaxMindDBError</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.eq\" class=\"method trait-impl\"><a href=\"#method.eq\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/cmp/trait.PartialEq.html#tymethod.eq\" class=\"fn\">eq</a>(&amp;self, other: &amp;MaxMindDBError) -&gt; <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.bool.html\">bool</a></h4></section></summary><div class='docblock'>Tests for <code>self</code> and <code>other</code> values to be equal, and is used by <code>==</code>.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.ne\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.82.0/src/core/cmp.rs.html#261\">source</a></span><a href=\"#method.ne\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.82.0/core/cmp/trait.PartialEq.html#method.ne\" class=\"fn\">ne</a>(&amp;self, other: <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.reference.html\">&amp;Rhs</a>) -&gt; <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.82.0/std/primitive.bool.html\">bool</a></h4></section></summary><div class='docblock'>Tests for <code>!=</code>. The default implementation is almost always sufficient,\nand should not be overridden without very good reason.</div></details></div></details>","PartialEq","relay_event_normalization::geo::GeoIpError"],["<section id=\"impl-Eq-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-Eq-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/cmp/trait.Eq.html\" title=\"trait core::cmp::Eq\">Eq</a> for MaxMindDBError</h3></section>","Eq","relay_event_normalization::geo::GeoIpError"],["<section id=\"impl-StructuralPartialEq-for-MaxMindDBError\" class=\"impl\"><a href=\"#impl-StructuralPartialEq-for-MaxMindDBError\" class=\"anchor\">§</a><h3 class=\"code-header\">impl <a class=\"trait\" href=\"https://doc.rust-lang.org/1.82.0/core/marker/trait.StructuralPartialEq.html\" title=\"trait core::marker::StructuralPartialEq\">StructuralPartialEq</a> for MaxMindDBError</h3></section>","StructuralPartialEq","relay_event_normalization::geo::GeoIpError"]]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[19265]}
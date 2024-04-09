(function() {var type_impls = {
"relay_cardinality":[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-CardinalityLimiter%3CT%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/relay_cardinality/limiter.rs.html#137-185\">source</a><a href=\"#impl-CardinalityLimiter%3CT%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;T: <a class=\"trait\" href=\"relay_cardinality/limiter/trait.Limiter.html\" title=\"trait relay_cardinality::limiter::Limiter\">Limiter</a>&gt; <a class=\"struct\" href=\"relay_cardinality/limiter/struct.CardinalityLimiter.html\" title=\"struct relay_cardinality::limiter::CardinalityLimiter\">CardinalityLimiter</a>&lt;T&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.new\" class=\"method\"><a class=\"src rightside\" href=\"src/relay_cardinality/limiter.rs.html#139-141\">source</a><h4 class=\"code-header\">pub fn <a href=\"relay_cardinality/limiter/struct.CardinalityLimiter.html#tymethod.new\" class=\"fn\">new</a>(limiter: T) -&gt; Self</h4></section></summary><div class=\"docblock\"><p>Creates a new cardinality limiter.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.check_cardinality_limits\" class=\"method\"><a class=\"src rightside\" href=\"src/relay_cardinality/limiter.rs.html#146-184\">source</a><h4 class=\"code-header\">pub fn <a href=\"relay_cardinality/limiter/struct.CardinalityLimiter.html#tymethod.check_cardinality_limits\" class=\"fn\">check_cardinality_limits</a>&lt;'a, I: <a class=\"trait\" href=\"relay_cardinality/limiter/trait.CardinalityItem.html\" title=\"trait relay_cardinality::limiter::CardinalityItem\">CardinalityItem</a>&gt;(\n    &amp;self,\n    scoping: <a class=\"struct\" href=\"relay_cardinality/limiter/struct.Scoping.html\" title=\"struct relay_cardinality::limiter::Scoping\">Scoping</a>,\n    limits: &amp;'a [<a class=\"struct\" href=\"relay_cardinality/struct.CardinalityLimit.html\" title=\"struct relay_cardinality::CardinalityLimit\">CardinalityLimit</a>],\n    items: <a class=\"struct\" href=\"https://doc.rust-lang.org/1.77.2/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;I&gt;\n) -&gt; <a class=\"type\" href=\"relay_cardinality/type.Result.html\" title=\"type relay_cardinality::Result\">Result</a>&lt;<a class=\"struct\" href=\"relay_cardinality/limiter/struct.CardinalityLimits.html\" title=\"struct relay_cardinality::limiter::CardinalityLimits\">CardinalityLimits</a>&lt;'a, I&gt;, (<a class=\"struct\" href=\"https://doc.rust-lang.org/1.77.2/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;I&gt;, <a class=\"enum\" href=\"relay_cardinality/enum.Error.html\" title=\"enum relay_cardinality::Error\">Error</a>)&gt;</h4></section></summary><div class=\"docblock\"><p>Checks cardinality limits of a list of buckets.</p>\n<p>Returns an iterator of all buckets that have been accepted.</p>\n</div></details></div></details>",0,"relay_cardinality::CardinalityLimiter"]]
};if (window.register_type_impls) {window.register_type_impls(type_impls);} else {window.pending_type_impls = type_impls;}})()
(function() {var type_impls = {
"relay_event_schema":[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-ProcessValue-for-BTreeMap%3CString,+Annotated%3CT%3E%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/relay_event_schema/processor/impls.rs.html#156-197\">source</a><a href=\"#impl-ProcessValue-for-BTreeMap%3CString,+Annotated%3CT%3E%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;T&gt; <a class=\"trait\" href=\"relay_event_schema/processor/trait.ProcessValue.html\" title=\"trait relay_event_schema::processor::ProcessValue\">ProcessValue</a> for <a class=\"type\" href=\"relay_protocol/value/type.Object.html\" title=\"type relay_protocol::value::Object\">Object</a>&lt;T&gt;<div class=\"where\">where\n    T: <a class=\"trait\" href=\"relay_event_schema/processor/trait.ProcessValue.html\" title=\"trait relay_event_schema::processor::ProcessValue\">ProcessValue</a>,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.value_type\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/relay_event_schema/processor/impls.rs.html#161-163\">source</a><a href=\"#method.value_type\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"relay_event_schema/processor/trait.ProcessValue.html#method.value_type\" class=\"fn\">value_type</a>(&amp;self) -&gt; <a class=\"struct\" href=\"relay_event_schema/processor/struct.EnumSet.html\" title=\"struct relay_event_schema::processor::EnumSet\">EnumSet</a>&lt;<a class=\"enum\" href=\"relay_event_schema/processor/enum.ValueType.html\" title=\"enum relay_event_schema::processor::ValueType\">ValueType</a>&gt;</h4></section></summary><div class='docblock'>Returns the type of the value.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.process_value\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/relay_event_schema/processor/impls.rs.html#166-176\">source</a><a href=\"#method.process_value\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"relay_event_schema/processor/trait.ProcessValue.html#method.process_value\" class=\"fn\">process_value</a>&lt;P&gt;(\n    &amp;mut self,\n    meta: &amp;mut <a class=\"struct\" href=\"relay_protocol/meta/struct.Meta.html\" title=\"struct relay_protocol::meta::Meta\">Meta</a>,\n    processor: <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.80.0/std/primitive.reference.html\">&amp;mut P</a>,\n    state: &amp;<a class=\"struct\" href=\"relay_event_schema/processor/struct.ProcessingState.html\" title=\"struct relay_event_schema::processor::ProcessingState\">ProcessingState</a>&lt;'_&gt;,\n) -&gt; <a class=\"type\" href=\"relay_event_schema/processor/type.ProcessingResult.html\" title=\"type relay_event_schema::processor::ProcessingResult\">ProcessingResult</a><div class=\"where\">where\n    P: <a class=\"trait\" href=\"relay_event_schema/processor/trait.Processor.html\" title=\"trait relay_event_schema::processor::Processor\">Processor</a>,</div></h4></section></summary><div class='docblock'>Executes a processor on this value.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.process_child_values\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/relay_event_schema/processor/impls.rs.html#179-196\">source</a><a href=\"#method.process_child_values\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"relay_event_schema/processor/trait.ProcessValue.html#method.process_child_values\" class=\"fn\">process_child_values</a>&lt;P&gt;(\n    &amp;mut self,\n    processor: <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.80.0/std/primitive.reference.html\">&amp;mut P</a>,\n    state: &amp;<a class=\"struct\" href=\"relay_event_schema/processor/struct.ProcessingState.html\" title=\"struct relay_event_schema::processor::ProcessingState\">ProcessingState</a>&lt;'_&gt;,\n) -&gt; <a class=\"type\" href=\"relay_event_schema/processor/type.ProcessingResult.html\" title=\"type relay_event_schema::processor::ProcessingResult\">ProcessingResult</a><div class=\"where\">where\n    P: <a class=\"trait\" href=\"relay_event_schema/processor/trait.Processor.html\" title=\"trait relay_event_schema::processor::Processor\">Processor</a>,</div></h4></section></summary><div class='docblock'>Recurses into children of this value.</div></details></div></details>","ProcessValue","relay_event_schema::protocol::metrics_summary::MetricSummaryMapping"]]
};if (window.register_type_impls) {window.register_type_impls(type_impls);} else {window.pending_type_impls = type_impls;}})()
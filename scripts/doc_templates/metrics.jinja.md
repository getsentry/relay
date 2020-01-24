# Metrics

Relay emits statsd metrics in order to allow its monitoring.

{% for metric_type in all_metrics or []%}
## {{metric_type.name}}
    
{% for line in metric_type.description or [] %}{{line}}
{% endfor %}
{% for metric in metric_type.metrics or [] %}

### `{{metric.id}}`

{% if metric.feature %}
**Note**: This metric is emitted only when Relay is built with the `{{metric.feature}}` feature.
{% endif %}  

{% if metric.name %}
The metric is referred in code as: `{{metric_type.name}}::{{metric.name}}`.
{% endif %}
{% for line in metric.description or [] %}{{line}}
{% endfor %}
{% endfor %}
{% endfor %}

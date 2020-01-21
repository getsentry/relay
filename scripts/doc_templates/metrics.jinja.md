# Metrics

Relay emits statsd metrics in order to allow its monitoring.

{% for metric_type in all_metrics or []%}
## {{metric_type.name}}
    
{% for line in metric_type.description or [] %}{{line}}
{% endfor %}
{% for metric in metric_type.metrics or [] %}

### `{{metric.id}}`  ({{metric.name}})

{% for line in metric.description or [] %}{{line}}
{% endfor %}
{% endfor %}
{% endfor %}

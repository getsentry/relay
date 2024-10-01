---
name: Epic
about: Plan a new initative
title: '[EPIC] <title>'
labels: ''
assignees: ''

---

## Background

Add context and motivation here.

## Requirements

What is the desired outcome of this initiative? What are the hard technical requirements.
Add measurable success criteria here.

## Design

Describe how you want to meet the requirements above.

## Infrastructure checklist

Be sure to assess the infrastructure impact of your initiative early on, such that
the required work can be planned in advance.

- [ ] Will this affect CPU and / or memory usage?
- [ ] Does this plan require different machine types (different memory to CPU ratio)?
- [ ] Does this plan require new autoscaling rules? Will it impact the metric that the autoscaler uses?
- [ ] Will this increase the load on redis, kafka, or the HTTP upstream?
- [ ] Does this plan require larger disks or different disk types?
- [ ] Will it affect health checks and/or HTTP response times?

## Implementation

Once the planning is done, list implementation tasks here.

```[tasklist]
### Implementation tasks
- [ ] ...
```

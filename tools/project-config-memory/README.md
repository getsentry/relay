# Estimate memory footprint of project configs

## Usage

```bash
project-config-memory filename.json
```

## Method

Select 100 random projects:

```sql
select * from sentry_project order by random() limit 100;
```

Fetch configs from https://us.sentry.io/api/0/internal/project-config/?projectId=<id>. Most projects don't have an active project config so this takes a while.

Run the tool.

```bash
cargo build
for f in ~/Desktop/project-configs/*json; do
  ../../target/debug/memory-flame-chart $f;
done > output.csv
```

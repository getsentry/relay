local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

{
  pause_on_failure(): {
    plugin: {
      options: gocdtasks.script(importstr '../bash/pause-current-pipeline.sh'),
      run_if: 'failed',
      configuration: {
        id: 'script-executor',
        version: 1,
      },
    },
  },
  github_checks(): [
    {
      checks: {
        fetch_materials: true,
        jobs: {
          checks: {
            environment_variables: {
              GITHUB_TOKEN: '{{SECRET:[devinfra-github][token]}}',
            },
            timeout: 1800,
            elastic_profile_id: 'relay',
            tasks: [
              gocdtasks.script(importstr '../bash/github-check-runs.sh'),
            ],
          },
        },
      },
    },
  ],
}

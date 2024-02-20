local pops = import './relay-pops-old.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

function(region) {
  environment_variables: {
    SENTRY_REGION: region,
  },
  group: 'relay-next',
  lock_behavior: 'unlockWhenFinished',
  materials: {
    relay_repo: {
      git: 'git@github.com:getsentry/relay.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'relay',
    },
  },
  stages: [

    // Check that github status is good
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

    // Deploy relay
    {
      'deploy-production': {
        fetch_materials: true,
        jobs: {
          create_sentry_release: {
            environment_variables: {
              SENTRY_ORG: 'sentry',
              SENTRY_PROJECT: 'relay',
              SENTRY_URL: 'https://sentry.my.sentry.io/',
              // Temporary; self-service encrypted secrets aren't implemented yet.
              // This should really be rotated to an internal integration token.
              SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-temp][relay_sentry_auth_token]}}',
            },
            timeout: 1200,
            elastic_profile_id: 'relay',
            tasks: [
              gocdtasks.script(importstr '../bash/create-sentry-relay-release.sh'),
            ],
          },
          create_sentry_st_release: {
            environment_variables: {
              SENTRY_ORG: 'sentry-st',
              SENTRY_PROJECT: 'sentry-for-sentry',
              SENTRY_URL: 'https://sentry-st.sentry.io/',
              // Temporary; self-service encrypted secrets aren't implemented yet.
              // This should really be rotated to an internal integration token.
              SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-temp][relay_sentry_st_auth_token]}}',
            },
            timeout: 1200,
            elastic_profile_id: 'relay',
            tasks: [
              gocdtasks.script(importstr '../bash/create-sentry-relay-release.sh'),
            ],
          },
          deploy: {
            timeout: 1200,
            elastic_profile_id: 'relay',
            tasks: [
              gocdtasks.script(importstr '../bash/deploy-relay.sh'),
            ],
          },
        },
      },
    },
  ] + pops.stages(region),
}

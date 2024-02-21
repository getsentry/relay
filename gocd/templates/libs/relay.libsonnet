local utils = import './utils.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

// The purpose of this stage is to let the deployment soak for a while and
// detect any issues that might have been introduced.
local soak_time(region) =
  if region == 's4s' || region == 'us' then
    [
      {
        'soak-time': {
          jobs: {
            soak: {
              environment_variables: {
                SENTRY_REGION: region,
                GOCD_ACCESS_TOKEN: '{{SECRET:[devinfra][gocd_access_token]}}',
                SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
                DATADOG_API_KEY: '{{SECRET:[devinfra][sentry_datadog_api_key]}}',
                DATADOG_APP_KEY: '{{SECRET:[devinfra][sentry_datadog_app_key]}}',
                // Datadog monitor IDs for the soak time
                // TODO: Add the monitor IDs
                DATADOG_MONITOR_IDS: '137566884 14146876 137619914 14030245',
                SENTRY_PROJECT: 'relay',
                SENTRY_PROJECT_ID: '4',
                SENTRY_SINGLE_TENANT: 'false',
                // TODO: Set a proper error limit
                ERROR_LIMIT: 500,
                PAUSE_MESSAGE: 'Detecting issues in the deployment. Pausing pipeline.',
                // TODO: Switch dry run to false once we're confident in the soak time
                DRY_RUN: 'true',
              },
              elastic_profile_id: 'relay',
              tasks: [
                gocdtasks.script(importstr '../bash/wait-soak.sh'),
                gocdtasks.script(importstr '../bash/check-sentry-errors.sh'),
                gocdtasks.script(importstr '../bash/check-sentry-new-errors.sh'),
                gocdtasks.script(importstr '../bash/check-datadog-status.sh'),
                utils.pause_on_failure(),
              ],
            },
          },
        },
      },
    ]
  else
    [];

// The purpose of this stage is to deploy a canary for a given region and wait for a few minutes
// to see if there are any issues.
local deploy_canary(region) =
  if region == 'us' then
    [
      {
        'deploy-canary': {
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
                SENTRY_ENVIRONMENT: 'canary',
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
              environment_variables: {
                SENTRY_REGION: region,
                GOCD_ACCESS_TOKEN: '{{SECRET:[devinfra][gocd_access_token]}}',
                SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
                DATADOG_API_KEY: '{{SECRET:[devinfra][sentry_datadog_api_key]}}',
                DATADOG_APP_KEY: '{{SECRET:[devinfra][sentry_datadog_app_key]}}',
                // Datadog monitor IDs for the canary deployment
                DATADOG_MONITOR_IDS: '137566884 14146876 137619914 14030245',
                SENTRY_PROJECT: 'relay',
                SENTRY_PROJECT_ID: '4',
                SENTRY_SINGLE_TENANT: 'false',
                // TODO: Set a proper error limit
                ERROR_LIMIT: 500,
                PAUSE_MESSAGE: 'Pausing pipeline due to canary failure.',
                // TODO: Switch dry run to false once we're confident in the canary
                DRY_RUN: 'true',
              },
              tasks: [
                gocdtasks.script(importstr '../bash/deploy-relay-canary.sh'),
                gocdtasks.script(importstr '../bash/wait-canary.sh'),
                gocdtasks.script(importstr '../bash/check-datadog-status.sh'),
                utils.pause_on_failure(),
              ],
            },
          },
        },
      },
    ]
  else
    [];

// The purpose of this stage is to deploy to production
local deploy_primary(region) = [
  {
    'deploy-primary': {
      fetch_materials: true,
      jobs: {
        create_sentry_release: {
          environment_variables: {
            SENTRY_ORG: if region == 's4s' then 'sentry-st' else 'sentry',
            SENTRY_PROJECT: if region == 's4s' then 'sentry-for-sentry' else 'relay',
            SENTRY_URL: if region == 's4s' then 'https://sentry-st.sentry.io/' else 'https://sentry.my.sentry.io/',
            // Temporary; self-service encrypted secrets aren't implemented yet.
            // This should really be rotated to an internal integration token.
            SENTRY_AUTH_TOKEN: if region == 's4s' then '{{SECRET:[devinfra-temp][relay_sentry_st_auth_token]}}' else '{{SECRET:[devinfra-temp][relay_sentry_auth_token]}}',
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
];


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
  stages: utils.github_checks() + deploy_canary(region) + deploy_primary(region) + soak_time(region),
}

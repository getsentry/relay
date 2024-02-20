local utils = import './utils.libsonnet';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

local canary_region_pops = {
  de: [],
  // TODO: Check that these are right
  us: ['us-pop-1', 'us-pop-regional-1'],
};

local region_pops = {
  de: [
    'de-pop-1',
    'de-pop-2',
  ],
  us: [
    'us-pop-1',
    'us-pop-2',
    'us-pop-3',
    'us-pop-4',
    'us-pop-regional-1',
    'us-pop-regional-2',
    'us-pop-regional-3',
    'us-pop-regional-4',
  ],
};

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
                DATADOG_MONITOR_IDS: '137575470 22592147 27804625 22634395 22635255',
                SENTRY_PROJECT: 'pop-relay',
                SENTRY_PROJECT_ID: '9',
                SENTRY_SINGLE_TENANT: 'false',
                // TODO: Set a proper error limit
                ERROR_LIMIT: 500,
                PAUSE_MESSAGE: 'Detecting issues in the deployment. Pausing pipeline.',
                // TODO: Switch dry run to false once we're confident in the soak time
                DRY_RUN: 'true',
              },
              elastic_profile_id: 'relay-pop',
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

// Create a gocd job that will run the deploy-pop-canary script,
// wait for a few minutes, and check the status of the canary deployment.
local deploy_pop_canary_job(region) =
  {
    timeout: 1200,
    elastic_profile_id: 'relay-pop',
    environment_variables: {
      SENTRY_REGION: region,
      GOCD_ACCESS_TOKEN: '{{SECRET:[devinfra][gocd_access_token]}}',
      SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-sentryio][token]}}',
      DATADOG_API_KEY: '{{SECRET:[devinfra][sentry_datadog_api_key]}}',
      DATADOG_APP_KEY: '{{SECRET:[devinfra][sentry_datadog_app_key]}}',
      // Datadog monitor IDs for the canary deployment
      DATADOG_MONITOR_IDS: '137575470 22592147 27804625 22634395 22635255',
      SENTRY_PROJECT: 'pop-relay',
      SENTRY_PROJECT_ID: '9',
      SENTRY_SINGLE_TENANT: 'false',
      // TODO: Set a proper error limit
      ERROR_LIMIT: 500,
      PAUSE_MESSAGE: 'Pausing pipeline due to canary failure.',
      // TODO: Switch dry run to false once we're confident in the soak time
      DRY_RUN: 'true',
    },
    tasks: [
      gocdtasks.script(importstr '../bash/deploy-pop-canary.sh'),
      gocdtasks.script(importstr '../bash/wait-canary.sh'),
      gocdtasks.script(importstr '../bash/check-sentry-errors.sh'),
      gocdtasks.script(importstr '../bash/check-sentry-new-errors.sh'),
      gocdtasks.script(importstr '../bash/check-datadog-status.sh'),
      utils.pause_on_failure(),
    ],
  };

// Create a gocd job that will run the deploy-pop script
local deploy_pop_job(region) =
  {
    timeout: 1200,
    elastic_profile_id: 'relay-pop',
    environment_variables: {
      SENTRY_REGION: region,
    },
    tasks: [
      gocdtasks.script(importstr '../bash/deploy-pop.sh'),
    ],
  };

// Iterate over a list of regions and create a job for each
local deploy_jobs(regions, deploy_job, partition='-') =
  {
    ['deploy-primary' + partition + region]: deploy_job(region)
    for region in regions
  };

// The purpose of this stage is to deploy a canary to all canary PoPs for a given region
// and wait for a few minutes to see if there are any issues.
local deploy_canary_pops_stage(region) =
  {
    'deploy-canary'+: {
      fetch_materials: true,
      jobs+: deploy_jobs(
        [region] + canary_region_pops[region],
        deploy_pop_canary_job,
        '-canary-',
      ),
    },
  };

// The purpose of this stage is to deploy to all PoPs for a given region as well
// as create a sentry release.
local deploy_pops_stage(region) =
  {
    'deploy-primary': {
      fetch_materials: true,
      jobs: {
        // PoPs have their own Sentry project, which requires separate symbol upload via
        // create-sentry-release. They could be moved into the same project with a different
        // environment to avoid this.
        create_sentry_release: {
          timeout: 1200,
          elastic_profile_id: 'relay',
          environment_variables: {
            SENTRY_ORG: 'sentry',
            SENTRY_PROJECT: 'pop-relay',
            SENTRY_URL: 'https://sentry.my.sentry.io/',
            // Temporary; self-service encrypted secrets aren't implemented yet.
            // This should really be rotated to an internal integration token.
            SENTRY_AUTH_TOKEN: '{{SECRET:[devinfra-temp][relay_sentry_auth_token]}}',
          },
          tasks: [
            gocdtasks.script(importstr '../bash/create-sentry-relay-pop-release.sh'),
          ],
        },
      },
    },
  } {
    'deploy-primary'+: {
      jobs+: deploy_jobs(
        [region] + region_pops[region],
        deploy_pop_job,
      ),
    },
  };

// The purpose of this stage is to deploy to a single PoP for a given region.
local deploy_generic_pops_stage(region) =
  {
    'deploy-primary': {
      fetch_materials: true,
      jobs: {
        ['deploy-primary-' + region]: deploy_pop_job(region),
      },
    },
  };

// The US region deploys create a sentry release and deploys to a number
// of clusters, other regions only deploy to a single cluster.
local deployment_stages(region) =
  if region == 'us' || region == 'de' then
    // The canary stage is only for the US and DE regions
    [deploy_canary_pops_stage(region), deploy_pops_stage(region)]
  else
    [deploy_generic_pops_stage(region)];


function(region) {
  environment_variables: {
    SENTRY_REGION: region,
  },
  group: 'relay-pops-next',
  lock_behavior: 'unlockWhenFinished',
  materials: {
    relay_repo: {
      git: 'git@github.com:getsentry/relay.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'relay',
    },
  },
  stages: utils.github_checks() + deployment_stages(region) + soak_time(region),
}

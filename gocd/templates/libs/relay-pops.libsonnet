local STAGE_NAME = 'deploy-pops';
local gocdtasks = import 'github.com/getsentry/gocd-jsonnet/libs/gocd-tasks.libsonnet';

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
local deploy_pop_jobs(regions) =
  {
    [STAGE_NAME + '-' + region]: deploy_pop_job(region)
    for region in regions
  };

local us_pops_stage() =
  {
    [STAGE_NAME]: {
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
            gocdtasks.script(importstr '../bash/create-sentry-release.sh'),
          ],
        },
      },
    },
  } {
    [STAGE_NAME]+: {
      jobs+: deploy_pop_jobs([
        'us-pop-1',
        'us-pop-2',
        'us-pop-3',
        'us-pop-4',
      ]),
    },
  };

local generic_pops_stage(region) =
  {
    [STAGE_NAME]: {
      fetch_materials: true,
      jobs: deploy_pop_jobs([region]),
    },
  };

// The US region deploys create a sentry release and deploys to a number
// of clusters, other regions only deploy to a single cluster.
{
  stages(region)::
    if region == 'us' then
      [us_pops_stage()]
    else
      [generic_pops_stage(region)],
}

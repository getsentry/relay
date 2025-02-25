// Learn more about GoCD Pipedream here:
// https://www.notion.so/sentry/Pipedreams-in-GoCD-with-Jsonnet-430f46b87fa14650a80adf6708b088d9

local pops = import './pipelines/pops.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local pipedream_config = {
  name: 'relay-pop',
  auto_deploy: false,
  exclude_regions: [
    'customer-1',
    'customer-2',
    'customer-4',
  ],
  materials: {
    relay_repo: {
      git: 'git@github.com:getsentry/relay.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'relay',
    },
  },
  rollback: {
    material_name: 'relay_repo',
    stage: 'deploy-primary',
    elastic_profile_id: 'relay-pop',
  },
};

pipedream.render(pipedream_config, pops)

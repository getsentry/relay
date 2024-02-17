// Learn more about GoCD Pipedream here:
// https://www.notion.so/sentry/Pipedreams-in-GoCD-with-Jsonnet-430f46b87fa14650a80adf6708b088d9

local pops = import './libs/pops.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local pipedream_config = {
  name: 'relay-pops',
  auto_deploy: false,
  materials: {
    relay_repo: {
      git: 'git@github.com:getsentry/relay.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'relay',
    },
  },
};

pipedream.render(pipedream_config, pops)

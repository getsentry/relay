local relay = import './libs/relay.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/v1.0.0/pipedream.libsonnet';

local pipedream_config = {
  name: 'relay-next',
  auto_deploy: false,
  auto_pipeline_progression: false,
  materials: {
    relay_repo: {
      git: 'git@github.com:getsentry/relay.git',
      shallow_clone: true,
      branch: 'master',
      destination: 'relay',
    },
  },
};

pipedream.render(pipedream_config, relay.pipeline)

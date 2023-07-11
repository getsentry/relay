local relay = import './libs/relay.libsonnet';
local pipedream = import 'github.com/getsentry/gocd-jsonnet/libs/pipedream.libsonnet';

local pipedream_config = {
  name: 'relay-next',
  auto_deploy: false,
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
    stage: 'deploy-production',
  },
};

pipedream.render(pipedream_config, relay)

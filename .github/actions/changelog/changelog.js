const core = require('@actions/core');
const gh = require('@actions/github');

const context = gh.context;
const oktokit = gh.getOctokit(process.env.GITHUB_TOKEN);

const PR_LINK = `[#${context.payload.pull_request.number}](${context.payload.pull_request.html_url})`;

function getCleanTitle(title) {
  // remove fix(component): prefix
  title = title.split(': ').slice(-1)[0].trim();
  // remove links to JIRA tickets, i.e. a suffix like [ISSUE-123]
  title = title.split('[')[0].trim();
  // remove trailing dots
  title = title.replace(/\.+$/, '');

  return title;
}

function getChangelogDetails(title) {
  return `
For changes exposed to the _Python package_, please add an entry to \`py/CHANGELOG.md\`. This includes, but is not limited to event normalization, PII scrubbing, and the protocol.
For changes to the _Relay server_, please add an entry to \`CHANGELOG.md\` under the following heading:
 1. **Features**: For new user-visible functionality.
 2. **Bug Fixes**: For user-visible bug fixes.
 3. **Internal**: For features and bug fixes in internal operation, especially processing mode.
To the changelog entry, please add a link to this PR (consider a more descriptive message):
\`\`\`md
- ${title}. (${PR_LINK})
\`\`\`
If none of the above apply, you can opt out by adding _#skip-changelog_ to the PR description.
`;
}

async function containsChangelog(path) {
  const {data} = await oktokit.rest.repos.getContent({
    owner: context.repo.owner,
    repo: context.repo.repo,
    ref: context.ref,
    path,
  })
  const buf = Buffer.alloc(data.content.length, data.content, data.encoding);
  const fileContent = buf.toString();
  return fileContent.includes(PR_LINK);
}

async function checkChangelog(pr) {
  if ((pr.body || '').includes('#skip-changelog')) {
    core.info('#skip-changelog is set. Skipping the checks.');
    return
  }

  const hasChangelog =
    (await containsChangelog('CHANGELOG.md')) ||
    (await containsChangelog('py/CHANGELOG.md'));

  if (!hasChangelog) {
    core.error('Please consider adding a changelog entry for the next release.', {
      title: 'Missing changelog entry.',
      file: 'CHANGELOG.md',
      startLine: 3,
    })
    const title = getCleanTitle(pr.title);
    core.summary
      .addHeading('Instructions and example for changelog')
      .addRaw(getChangelogDetails(title))
      .write();
    core.setFailed('CHANGELOG entry is missing.');
    return
  }

  core.summary.clear();
  core.info("CHANGELOG entry is added, we're good to go.");
}

async function checkAll() {
  const {data: pr} = await oktokit.rest.pulls.get({
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: context.payload.pull_request.number,
  })

  if (pr.merged || pr.draft) {
    return;
  }

  await checkChangelog(pr);
}

checkAll();

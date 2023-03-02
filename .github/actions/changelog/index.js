module.exports = async ({github, context, core}) => {
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

  function logOutputError(title) {
    core.info('');
    core.info('\u001b[1mInstructions and example for changelog');
    core.info(getChangelogDetails(title));
    core.info('');
    core.info('\u001b[1mSee check status:');
    core.info(
      `https://github.com/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`
    );
  }

  async function containsChangelog(path) {
    const {data} = await github.rest.repos.getContent({
      owner: context.repo.owner,
      repo: context.repo.repo,
      ref: context.ref,
      path,
    });
    const buf = Buffer.alloc(data.content.length, data.content, data.encoding);
    const fileContent = buf.toString();
    return fileContent.includes(PR_LINK);
  }

  async function checkChangelog(pr) {
    if ((pr.body || '').includes('#skip-changelog')) {
      core.info('#skip-changelog is set. Skipping the checks.');
      return;
    }

    const hasChangelog =
      (await containsChangelog('CHANGELOG.md')) ||
      (await containsChangelog('py/CHANGELOG.md'));

    if (!hasChangelog) {
      core.error('Please consider adding a changelog entry for the next release.', {
        title: 'Missing changelog entry.',
        file: 'CHANGELOG.md',
        startLine: 3,
      });
      const title = getCleanTitle(pr.title);
      core.summary
        .addHeading('Instructions and example for changelog')
        .addRaw(getChangelogDetails(title))
        .write();
      core.setFailed('CHANGELOG entry is missing.');
      logOutputError(title);
      return;
    }

    core.summary.clear();
    core.info("CHANGELOG entry is added, we're good to go.");
  }

  async function checkAll() {
    const {data: pr} = await github.rest.pulls.get({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: context.payload.pull_request.number,
    });

   // While in draft mode, skip the check because changelogs often cause merge conflicts.
   if (pr.merged || pr.draft) {
      return;
    }

    await checkChangelog(pr);
  }

  await checkAll();
};

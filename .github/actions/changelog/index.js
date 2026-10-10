const {execFileSync} = require('child_process');
const path = require('path');

module.exports = async ({ github, context, core }) => {
  const PR_LINK = `[#${context.payload.pull_request.number}](${context.payload.pull_request.html_url})`;

  // Crates the C ABI depends on for infrastructure only. Changes to them never reach Python.
  const INFRASTRUCTURE_CRATES = new Set([
    'relay-cogs',
    'relay-log',
    'relay-redis',
    'relay-statsd',
    'relay-system',
  ]);

  function getCleanTitle(title) {
    // remove fix(component): prefix
    title = title.split(': ').slice(-1)[0].trim();
    // remove links to JIRA tickets, i.e. a suffix like [ISSUE-123]
    title = title.split('[')[0].trim();
    // remove trailing dots
    title = title.replace(/\.+$/, '');

    return title;
  }

  function isRevert(title) {
    const REVERT_RE = /^Revert ".*"$/;
    return title && title.match(REVERT_RE) !== null;
  }

  function getChangelogDetails(title) {
    return `
  For changes exposed to the _Python package_, please add an entry to \`py/CHANGELOG.md\`. This includes, but is not limited to event normalization, PII scrubbing, and the protocol.
  Changes under \`py/\` or to a crate the C ABI depends on always require an entry in \`py/CHANGELOG.md\`.
  For changes to the _Relay server_, please add an entry to \`CHANGELOG.md\` under the following heading:
   1. **Features**: For new user-visible functionality.
   2. **Bug Fixes**: For user-visible bug fixes.
   3. **Internal**: For features and bug fixes in internal operation, especially processing mode.
  To the changelog entry, please add a link to this PR (consider a more descriptive message):
  \`\`\`md
  - ${title}. (${PR_LINK})
  \`\`\`
  If none of the above apply, you can opt out by adding the _skip-changelog_ label to the PR.
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
    const { data } = await github.rest.repos.getContent({
      owner: context.repo.owner,
      repo: context.repo.repo,
      ref: context.ref,
      path,
    });
    const buf = Buffer.alloc(data.content.length, data.content, data.encoding);
    const fileContent = buf.toString();
    return fileContent.match(/## Unreleased(.*?)##/ms)?.[1]?.includes(PR_LINK) || false;
  }

  // Directories whose files change what the Python package sees: the package itself and every
  // workspace crate the C ABI depends on. Falls back to the C ABI crate alone if cargo fails.
  function pythonInterfaceDirs() {
    try {
      const metadata = JSON.parse(
        execFileSync('cargo', ['metadata', '--no-deps', '--format-version', '1'], {
          encoding: 'utf8',
        })
      );
      const packages = new Map(metadata.packages.map(pkg => [pkg.name, pkg]));

      const closure = new Set();
      const todo = ['relay-cabi'];
      while (todo.length > 0) {
        const name = todo.pop();
        const pkg = packages.get(name);
        if (!pkg || closure.has(name)) {
          continue;
        }
        closure.add(name);
        for (const dep of pkg.dependencies) {
          if (dep.kind === null && packages.has(dep.name)) {
            todo.push(dep.name);
          }
        }
      }

      const dirs = [...closure]
        .filter(name => !INFRASTRUCTURE_CRATES.has(name))
        .map(name => path.dirname(packages.get(name).manifest_path))
        .map(dir => path.relative(metadata.workspace_root, dir));
      return ['py', ...dirs];
    } catch (error) {
      core.warning(`Cannot resolve the C ABI dependencies, checking relay-cabi only: ${error}`);
      return ['py', 'relay-cabi'];
    }
  }

  async function touchesPythonInterface() {
    const files = await github.paginate(github.rest.pulls.listFiles, {
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: context.payload.pull_request.number,
      per_page: 100,
    });
    const dirs = pythonInterfaceDirs();
    return files.some(
      file =>
        file.filename !== 'py/CHANGELOG.md' &&
        dirs.some(dir => file.filename.startsWith(`${dir}/`))
    );
  }

  function failMissingChangelog(pr, file, message) {
    core.error(message, {
      title: 'Missing changelog entry.',
      file,
      startLine: 3,
    });
    const title = getCleanTitle(pr.title);
    core.summary
      .addHeading('Instructions and example for changelog')
      .addRaw(getChangelogDetails(title))
      .write();
    core.setFailed(`${file} entry is missing.`);
    logOutputError(title);
  }

  async function checkChangelog(pr) {
    const hasSkipLabel = (pr.labels || []).some(label => label.name === 'skip-changelog');
    if (hasSkipLabel) {
      return;
    }

    if (isRevert(pr.title)) {
      return;
    }

    const hasPyChangelog = await containsChangelog('py/CHANGELOG.md');

    if (!hasPyChangelog && (await touchesPythonInterface())) {
      failMissingChangelog(
        pr,
        'py/CHANGELOG.md',
        'This PR changes the Python package or the C ABI. Please add an entry to py/CHANGELOG.md.'
      );
      return;
    }

    const hasChangelog = hasPyChangelog || (await containsChangelog('CHANGELOG.md'));

    if (!hasChangelog) {
      failMissingChangelog(
        pr,
        'CHANGELOG.md',
        'Please consider adding a changelog entry for the next release.'
      );
      return;
    }

    core.summary.clear();
    core.info("CHANGELOG entry is added, we're good to go.");
  }

  async function checkPrTitle(pr) {
    // Provide an opt out just in case, but this should never be used.
    const hasIgnoreLabel = (pr.labels || []).some(label => label.name === 'ignore-title');
    if (hasIgnoreLabel) {
      return;
    }

    // From: <https://develop.sentry.dev/engineering-practices/commit-messages/>.
    const TITLE_RE = /^(ci|build|docs|feat|fix|perf|ref|style|chore|test|meta|license)(\([^)]+\))?: [A-Z`'"].*[^,.]$/;

    if (pr.title.match(TITLE_RE) === null && !isRevert(pr.title)) {
      core.setFailed('PR title does not match Sentry conventions.');
      core.info('Please follow the Sentry commit message conventions: https://develop.sentry.dev/engineering-practices/commit-messages/');
      core.info('')
      core.info('Format: <type>(<scope>): <subject>');
      core.info('Subject line must be capitalized and must not end with a period.')
      return;
    }

    core.info("PR title matches Sentry conventions!");
  }

  async function checkAll() {
    const { data: pr } = await github.rest.pulls.get({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: context.payload.pull_request.number,
    });

    // While in draft mode, skip the check because changelogs often cause merge conflicts.
    if (pr.merged || pr.draft) {
      return;
    }

    await checkPrTitle(pr);
    await checkChangelog(pr);
  }

  await checkAll();
};

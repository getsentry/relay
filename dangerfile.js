const PR_NUMBER = danger.github.pr.number;
const PR_URL = danger.github.pr.html_url;
const PR_LINK = `[#${PR_NUMBER}](${PR_URL})`;

function getCleanTitle() {
  let title = danger.github.pr.title;
  // remove fix(component): prefix
  title = title.split(": ").slice(-1)[0].trim();
  // remove links to JIRA tickets, i.e. a suffix like [ISSUE-123]
  title = title.split("[")[0].trim();
  // remove trailing dots
  title = title.replace(/\.+$/, "");

  return title;
}

function getChangelogDetails() {
  return `
<details>
<summary><b>Instructions and example for changelog</b></summary>

For changes exposed to the _Python package_, please add an entry to \`py/CHANGELOG.md\`. This includes, but is not limited to event normalization, PII scrubbing, and the protocol.

For changes to the _Relay server_, please add an entry to \`CHANGELOG.md\` under the following heading:
 1. **Features**: For new user-visible functionality.
 2. **Bug Fixes**: For user-visible bug fixes.
 3. **Internal**: For features and bug fixes in internal operation, especially processing mode.

To the changelog entry, please add a link to this PR (consider a more descriptive message):

\`\`\`md
- ${getCleanTitle()}. (${PR_LINK})
\`\`\`

If none of the above apply, you can opt out by adding _#skip-changelog_ to the PR description.

</details>
`;
}

async function containsChangelog(path) {
  const contents = await danger.github.utils.fileContents(path);
  return contents.includes(PR_LINK);
}

async function checkChangelog() {
  const skipChangelog =
    danger.github && (danger.github.pr.body + "").includes("#skip-changelog");

  if (skipChangelog) {
    return;
  }

  const hasChangelog =
    (await containsChangelog("CHANGELOG.md")) ||
    (await containsChangelog("py/CHANGELOG.md"));

  if (!hasChangelog) {
    fail("Please consider adding a changelog entry for the next release.");
    markdown(getChangelogDetails());
  }
}

async function checkAll() {
  // See: https://spectrum.chat/danger/javascript/support-for-github-draft-prs~82948576-ce84-40e7-a043-7675e5bf5690
  const isDraft = danger.github.pr.mergeable_state === "draft";

  if (isDraft) {
    return;
  }

  await checkChangelog();
}

schedule(checkAll);

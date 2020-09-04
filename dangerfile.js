const prNumber = danger.github.pr.number;
const prUrl = danger.github.pr.html_url;
const prLink = `[#${prNumber}](${prUrl})`;

const ERROR_MESSAGE =
  "Please consider adding a changelog entry for the next release.";

const DETAILS = `
For changes exposed to the _Python package_, please add an entry to \`py/CHANGELOG.md\`. This includes, but is not limited to event normalization, PII scrubbing, and the protocol.

For changes to the _Relay server_, please add an entry to \`CHANGELOG.md\` under the following heading:
 1. **Features**: For new user-visible functionality.
 2. **Bug Fixes**: For user-visible bug fixes.
 3. **Internal**: For features and bug fixes in internal operation, especially processing mode.

To the changelog entry, please add a link to this PR:

\`\`\`md
${prLink}
\`\`\`

If none of the above apply, you can opt out by adding _#skip-changelog_ to the PR description.
`;

async function hasChangelog(path) {
  const contents = await danger.github.utils.fileContents(path);
  return contents.includes(prLink);
}

schedule(async () => {
  const skipChangelog =
    danger.github && (danger.github.pr.body + "").includes("#skip-changelog");

  if (skipChangelog) {
    return;
  }

  const hasChangelog =
    (await hasChangelog("CHANGELOG.md")) ||
    (await hasChangelog("py/CHANGELOG.md"));

  if (!hasChangelog) {
    fail(ERROR_MESSAGE);
    markdown(DETAILS);
  }
});

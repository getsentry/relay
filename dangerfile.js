const files = danger.git.modified_files;
const hasChangelog = files.indexOf("CHANGELOG.md") !== -1;
const hasPyChangelog = files.indexOf("py/CHANGELOG.md") !== -1;

const ERROR_MESSAGE =
  "Please consider adding a changelog entry for the next release.";

const DETAILS = `
For changes exposed to the _Python package_, please add an entry to \`py/CHANGELOG.md\`. This includes, but is not limited to event normalization, PII scrubbing, and the protocol.

For changes to the _Relay server_, please add an entry to \`CHANGELOG.md\` under the following heading:
 1. **Features**: For new user-visible functionality.
 2. **Bug Fixes**: For user-visible bug fixes.
 3. **Internal**: For features and bug fixes in internal operation, especially processing mode.

If none of the above apply, you can opt out by adding _#skip-changelog_ to the PR description.
`;

const skipChangelog =
  danger.github && (danger.github.pr.body + "").includes("#skip-changelog");

if (skipChangelog) {
  message("Opted out of changelogs due to #skip-changelog.");
} else if (!hasChangelog && !hasPyChangelog) {
  fail(ERROR_MESSAGE);
  markdown(DETAILS);
}

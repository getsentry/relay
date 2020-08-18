const files = danger.git.modified_files;
const hasChangelog = files.indexOf("CHANGELOG.md") !== -1;
const hasPyChangelog = files.indexOf("py/CHANGELOG.md") !== -1;

const ERROR_MESSAGE =
  "Please consider adding a changelog entry for the next release.";

const DETAILS = `
For changes exposed to the Python package, please add an entry to \`py/CHANGELOG.md\`. This includes, but is not limited to event normalization, PII scrubbing, and the protocol.

For changes to the Relay server, please add an entry to \`CHANGELOG.md\`:
 1. For new user-visible functionality under "Features"
 2. For user-visible bug fixes under "Bug Fixes"
 3. For features and bug fixes for internal operation (including processing mode) under "Internal"

If none of the above apply, you can opt out by adding #skip-changelog to the PR description.
`;

const skipChangelog =
  danger.github &&
  (danger.github.pr.body + danger.github.pr.title).includes("#skip-changelog");

if (!hasChangelog && !hasPyChangelog && !skipChangelog) {
  fail(ERROR_MESSAGE);
  markdown(DETAILS);
}

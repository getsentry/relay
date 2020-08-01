const hasChangelog = danger.git.modified_files.indexOf('CHANGELOG.md') !== -1;
const hasPyChangelog = danger.git.modified_files.indexOf('py/CHANGELOG.md') !== -1;

const skipChangelog = danger.github && (danger.github.pr.body + danger.github.pr.title).includes('#skip-changelog');

if (!hasChangelog && !hasPyChangelog && !skipChangelog) {
  warn('Please add a changelog entry in either CHANGELOG.md or py/CHANGELOG.md, or add #skip-changelog to the PR description or title.');
}

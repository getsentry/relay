import { danger, fail, message, schedule, warn } from 'danger';

export default async () => {
  if (!danger.github) {
    return;
  }

  const hasChangelog = danger.git.modified_files.indexOf('CHANGELOG.md') !== -1;
  const hasPyChangelog = danger.git.modified_files.indexOf('py/CHANGELOG.md') !== -1;

  const isTrivial = (danger.github.pr.body + danger.github.pr.title).includes('#trivial');

  if (!hasChangelog !hasPyChangelog && !isTrivial) {
    warn('Please add a changelog entry in either CHANGELOG.md or py/CHANGELOG.md, or add #trivial to the PR title.');
  }
}

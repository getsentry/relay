const core = require('@actions/core')
const gh = require('@actions/github')

const filesToCheck = ['CHANGELOG.md', 'py/CHANGELOG.md']

const context = gh.context
// We have to have the github token set for fetching PR description and content of the files.
const oktokit = gh.getOctokit(process.env.GITHUB_TOKEN)

// Get the PR link for current pull request.
const prLink = `([#${context.payload.pull_request.number}](${context.payload.pull_request.html_url}))`

// Make the title pretty.
function getCleanTitle (title) {
  // remove fix(component): prefix
  title = title.split(': ').slice(-1)[0].trim()
  // remove links to JIRA tickets, i.e. a suffix like [ISSUE-123]
  title = title.split('[')[0].trim()
  // remove trailing dots
  title = title.replace(/\.+$/, '')

  return title
}

// Checks if the file contains changelog entry, returns `true` if found or `false` otherwise.
async function changelogExists (path) {
  // Get the file content from the pull request branch.
  const { data } = await oktokit.rest.repos.getContent({ owner: context.repo.owner, repo: context.repo.repo, ref: context.ref, path })
  const buf = Buffer.alloc(data.content.length, data.content, data.encoding)
  const fileContent = buf.toString()

  if (core.isDebug()) {
    core.debug(`Checking file: ${path}.`)
    core.debug(`${fileContent}`)
  }

  return fileContent.includes(prLink)
}

async function main () {
  // Get the data for the current pull request.
  const { data: pr } = await oktokit.rest.pulls.get({ owner: context.repo.owner, repo: context.repo.repo, pull_number: context.payload.pull_request.number })

  // Ignore merged pull request or the ones which are still in draft.
  if (pr.merged || pr.draft) {
    return
  }

  const description = pr.body || ''

  // Skip check if pull request description has #skip-changelog.
  if (description.includes('#skip-changelog')) {
    core.info('#skip-changelog is set. Skipping the checks.')
    return
  }

  const exists = await Promise.all(filesToCheck.map(async (filePath) => {
    return await changelogExists(filePath)
  }))

  if (core.isDebug()) {
    core.debug(`Check results: ${exists.join(', ')}.`)
  }

  // Get and prepare the pull request title.
  const title = getCleanTitle(pr.title)
  // If one of the files contains the entry, we are ok with that.
  if (!exists.includes(true)) {
    core.error(`Consider adding the CHANGELOG entry for your change:\n\n${title}. ${prLink}`, { title: 'Missing changelog entry.', file: 'CHANGELOG.md', startLine: 3 })
    core.setFailed('CHANGELOG entry is missing.')
    return
  }

  core.info("CHANGELOG entry is added, we're good to go.")
}

main()

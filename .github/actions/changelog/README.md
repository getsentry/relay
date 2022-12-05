# Changelog Checker

This is the custom action to check if the changelog files contain the entry for the current pull request.

Currently we do not support any input or output state data and fully rely on the provided environment.


### Development

To make any contributions or changes to this code you must mamke sure that you have `node` installed. Once you have it, just
run `npm install` in folder to install all the dependencies. 

The entire Github action is located in `changelog.js` file. This file contain all the supported checks.

Once any changes are done, make sure to generated the compiled release file by running `npm run release`.
This will create `dist/index.js` file with all the used dependencies merged into one file, which later will be used
to run on the Github action worker.

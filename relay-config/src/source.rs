use std::collections::BTreeSet;
use std::path::PathBuf;

/// A custom [`serde_vars::source::FileSystem`], which loads files from the file system, but also
/// keeps track of the files read.
#[derive(Debug)]
pub struct TrackingFileSystem<'a>(pub &'a mut BTreeSet<PathBuf>);

impl<'a> serde_vars::source::FileSystem for TrackingFileSystem<'a> {
    fn read(&mut self, path: &std::path::Path) -> std::io::Result<Vec<u8>> {
        self.0.insert(path.to_owned());
        std::fs::read(path)
    }

    fn read_to_string(&mut self, path: &std::path::Path) -> std::io::Result<String> {
        self.0.insert(path.to_owned());
        std::fs::read_to_string(path)
    }
}

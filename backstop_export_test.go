package litestream

// SetBackstopStarted installs a delay at the start of this database's transport
// backstop watcher. Per instance: a package-global would be shared by every
// database in the process and could not survive parallel tests.
func (db *DB) SetBackstopStarted(fn func()) { db.backstopStarted = fn }

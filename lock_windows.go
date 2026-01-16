//go:build windows

package tinykvs

import (
	"os"
)

// acquireLock acquires an exclusive lock on the given file.
// On Windows, we rely on the file being opened exclusively.
// The LOCK file approach with exclusive open provides basic locking.
func acquireLock(f *os.File) error {
	// On Windows, opening with O_EXCL in the main Open function
	// combined with keeping the file handle open provides basic locking.
	// For stronger guarantees, use LockFileEx via syscall.
	return nil
}

// releaseLockFile releases the lock on the given file.
func releaseLockFile(f *os.File) {
	// No explicit unlock needed on Windows when using file handle approach
}

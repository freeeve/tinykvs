//go:build !windows

package tinykvs

import (
	"os"
	"syscall"
)

// acquireLock acquires an exclusive lock on the given file.
func acquireLock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// releaseLockFile releases the lock on the given file.
func releaseLockFile(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

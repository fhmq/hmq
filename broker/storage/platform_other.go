//go:build !linux && !darwin

package storage

import "os"

const platformSupported = false

func singleLink(*os.File) (bool, error) { return false, ErrUnsupported }

func lockFile(*os.File) error    { return ErrUnsupported }
func syncDirectory(string) error { return ErrUnsupported }

//go:build windows

package main

import (
	"os/exec"
	"strconv"
)

func prepareChild(_ *exec.Cmd) {}

func signalChild(cmd *exec.Cmd) error {
	return exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run()
}

func killChild(cmd *exec.Cmd) error {
	return signalChild(cmd)
}

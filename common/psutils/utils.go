package psutils

import (
	"github.com/aminvici/bds/common/pathutils"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

func WriteVersionFile(self, version, file string) (bool, error) {
	var f string
	switch runtime.GOOS {
	case "windows":
		p, err := pathutils.GetParentPath(self)
		if err != nil {
			return false, err
		}
		f = filepath.Join(p, "version")

	case "linux":
		p := file
		f = filepath.Join(p, "version")
	}
	if _, err := os.Stat(f); err == nil {
		err = os.Remove(f)
		if err != nil {
			return false, err
		}
	}
	w, err := os.Create(f)
	if err != nil {
		return false, err
	}
	w.Write([]byte(version))
	w.Close()
	return true, nil
}

func WritePIDFile(self, name, pid, file string) (bool, error) {
	var f string
	switch runtime.GOOS {
	case "windows":
		p, err := pathutils.GetParentPath(self)
		if err != nil {
			return false, err
		}
		f = filepath.Join(p, name)

	case "linux":
		path := file
		f = filepath.Join(path, name)
	}
	if _, err := os.Stat(f); err == nil {
		err = os.Remove(f)
		if err != nil {
			return false, err
		}
	}
	w, err := os.Create(f)
	if err != nil {
		return false, err
	}
	w.Write([]byte(pid))
	w.Close()
	return true, nil
}

func GetPID(self, name, file string) int {
	pid := 0
	pidStr := ""
	pidFile := ""
	switch runtime.GOOS {
	case "windows":
		p, err := pathutils.GetParentPath(self)
		if err != nil {
			return pid
		}
		pidFile = filepath.Join(p, name)
	case "linux":
		pidFile = filepath.Join(file, name)
	}
	if _, err := os.Stat(pidFile); err != nil {
		return pid
	}
	data, err := ioutil.ReadFile(pidFile)
	if err != nil {
		return pid
	}
	pidStr = strings.TrimSpace(string(data))
	pid, err = strconv.Atoi(pidStr)
	if err != nil {
		return pid
	}
	return pid
}

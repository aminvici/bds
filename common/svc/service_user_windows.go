// Copyright 2015 Daniel Theophanes.
// Use of this source code is governed by a zlib-style
// license that can be found in the LICENSE file.

package svc

import (
	"github.com/aminvici/bds/common/psutils"
	"golang.org/x/sys/windows/registry"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"time"
)

func init() {
	var err error
	interactive, err = svc.IsAnInteractiveSession()
	if err != nil {
		panic(err)
	}
}

func isUserWindows() bool {
	exepath := "TEST"
	k, err := registry.OpenKey(registry.CURRENT_USER, `SOFTWARE\Microsoft\Windows\CurrentVersion\Run`, registry.ALL_ACCESS)
	if err != nil {
		return false
	}
	defer k.Close()

	err = k.SetStringValue("TEST", exepath)
	if err != nil {
		return false
	}

	k, err = registry.OpenKey(registry.CURRENT_USER, `SOFTWARE\Microsoft\Windows\CurrentVersion\Run`, registry.ALL_ACCESS)
	if err != nil {
		return false
	}
	defer k.Close()

	err = k.DeleteValue("TEST")
	if err != nil {
		return false
	}
	return true
}

type userWindows struct {
	i Interface
	*Config
	pid          int
	errSync      sync.Mutex
	stopStartErr error
}

func newUserWindowsService(i Interface, c *Config) (Service, error) {
	s := &userWindows{
		i:      i,
		Config: c,
	}

	return s, nil
}

func (r *userWindows) String() string {
	if len(r.DisplayName) > 0 {
		return r.DisplayName
	}
	return r.Name
}

func (ws *userWindows) setError(err error) {
	ws.errSync.Lock()
	defer ws.errSync.Unlock()
	ws.stopStartErr = err
}
func (ws *userWindows) getError() error {
	ws.errSync.Lock()
	defer ws.errSync.Unlock()
	return ws.stopStartErr
}

func (ws *userWindows) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}

	if err := ws.i.Start(ws); err != nil {
		ws.setError(err)
		return true, 1
	}

	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}
loop:
	for {
		c := <-r
		switch c.Cmd {
		case svc.Interrogate:
			changes <- c.CurrentStatus
		case svc.Stop, svc.Shutdown:
			changes <- svc.Status{State: svc.StopPending}
			if err := ws.i.Stop(ws); err != nil {
				ws.setError(err)
				return true, 2
			}
			break loop
		default:
			continue loop
		}
	}

	return false, 0
}

func (ws *userWindows) Install() error {
	exepath, err := ws.execPath()
	if err != nil {
		return err
	}
	k, err := registry.OpenKey(registry.CURRENT_USER, `SOFTWARE\Microsoft\Windows\CurrentVersion\Run`, registry.ALL_ACCESS)
	if err != nil {
		return err
	}
	defer k.Close()
	vbepath := strings.Split(exepath, ".")[0] + ".vbe"

	err = k.SetStringValue(ws.Name, vbepath)
	if err != nil {
		return err
	}
	return nil
}

func (ws *userWindows) Uninstall() error {
	k, err := registry.OpenKey(registry.CURRENT_USER, `SOFTWARE\Microsoft\Windows\CurrentVersion\Run`, registry.ALL_ACCESS)
	if err != nil {
		return err
	}
	defer k.Close()

	err = k.DeleteValue(ws.Name)
	if err != nil {
		return err
	}
	return nil

}

func (ws *userWindows) Run() error {
	ws.setError(nil)
	if !interactive {
		// Return error messages from start and stop routines
		// that get executed in the Execute method.
		// Guarded with a mutex as it may run a different thread
		// (callback from windows).
		runErr := svc.Run(ws.Name, ws)
		startStopErr := ws.getError()
		if startStopErr != nil {
			return startStopErr
		}
		if runErr != nil {
			return runErr
		}
		return nil
	}
	err := ws.i.Start(ws)
	if err != nil {
		return err
	}
	sigChan := make(chan os.Signal)

	signal.Notify(sigChan, os.Interrupt, os.Kill)

	<-sigChan
	return ws.i.Stop(ws)
}

func (ws *userWindows) Start() error {
	exepath, err := ws.execPath()
	if err != nil {
		return err
	}
	vbepath := strings.Split(exepath, ".")[0] + ".vbe"

	cmd := exec.Command("cmd", "/c", "start", vbepath)
	err = cmd.Run()
	if err != nil {
		return err
	}

	/*_, err = psutils.WritePIDFile(os.Args[0], ws.Name, strconv.Itoa(os.Getpid()))
	if err != nil {
		return err
	}*/
	return nil
}

func (ws *userWindows) Stop() error {
	pid := psutils.GetPID(os.Args[0], ws.Name, "")
	pro, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	err = pro.Kill()
	if err != nil {
		return err
	}
	return nil
}

func (ws *userWindows) Restart() error {
	err := ws.Stop()
	if err != nil {
		return err
	}
	err = ws.Start()
	if err != nil {
		return err
	}
	return nil
}

func (ws *userWindows) stopWait(s *mgr.Service) error {
	// First stop the service. Then wait for the service to
	// actually stop before starting it.
	status, err := s.Control(svc.Stop)
	if err != nil {
		return err
	}

	timeDuration := time.Millisecond * 50

	timeout := time.After(getStopTimeout() + (timeDuration * 2))
	tick := time.NewTicker(timeDuration)
	defer tick.Stop()

	for status.State != svc.Stopped {
		select {
		case <-tick.C:
			status, err = s.Query()
			if err != nil {
				return err
			}
		case <-timeout:
			break
		}
	}
	return nil
}

func (ws *userWindows) Logger(errs chan<- error) (Logger, error) {
	if interactive {
		return ConsoleLogger, nil
	}
	return ws.SystemLogger(errs)
}
func (ws *userWindows) SystemLogger(errs chan<- error) (Logger, error) {
	el, err := eventlog.Open(ws.Name)
	if err != nil {
		return nil, err
	}
	return WindowsLogger{el, errs}, nil
}

package main

import (
	"flag"
	"fmt"
	jlog "github.com/jdcloud-bds/bds/common/jlog"
	"github.com/jdcloud-bds/bds/common/log"
	"github.com/jdcloud-bds/bds/common/metric"
	"github.com/jdcloud-bds/bds/common/svc"
	"github.com/jdcloud-bds/bds/config"
	"github.com/jdcloud-bds/bds/splitter"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
)

const (
	linuxDefaultConfigFile = "/etc/bds-splitter/splitter.conf"
)

var (
	versionNumber string
	buildTime     string
	configFile    string
	version       bool
)

func init() {
	flag.StringVar(&configFile, "c", linuxDefaultConfigFile, "config file path.")

	flag.BoolVar(&version, "v", false, "print version number.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\nusage: %s [install|uninstall|start|stop|restart]\n\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(0)
	}
	flag.Parse()
}

func profiling() {
	go func() {
		host := config.SplitterConfig.ProfilingSetting.Host
		port := config.SplitterConfig.ProfilingSetting.Port
		http.ListenAndServe(host+":"+port, nil)
	}()
}

func metrics() {
	go func() {
		host := config.SplitterConfig.MetricSetting.Host
		port := config.SplitterConfig.MetricSetting.Port
		path := config.SplitterConfig.MetricSetting.Path
		metric.NewMetricServer(host, port, path).Start()
	}()
}

type serviceWrapper struct{}

func (w *serviceWrapper) Start(s svc.Service) error {
	// Start should not block. Do the actual work async.
	go w.run()
	return nil
}
func (w *serviceWrapper) run() {
	// check version args
	if version {
		fmt.Fprintf(os.Stderr, "version: %s\n", versionNumber)
		fmt.Fprintf(os.Stderr, "build time: %s\n", buildTime)
		os.Exit(0)
	}
	// check config file
	if _, err := os.Stat(configFile); err != nil {
		fmt.Fprintf(os.Stderr, fmt.Sprintf("config file is not exist, please check %s.\n", configFile))
		os.Exit(2)
	}

	//init logger
	jlog.Registry(configFile)
	log.NormalLogger = jlog.GetLogger("normal")
	log.DetailLogger = jlog.GetLogger("detail")

	//parsing the config file
	if err := config.InitSplitterConfig(configFile); err != nil {
		log.Error("splitter: read config file error")
		log.DetailError(err)
		os.Exit(1)
	}

	runtime.GOMAXPROCS(config.SplitterConfig.GlobalSetting.MaxProcess)

	//go profile
	if config.SplitterConfig.ProfilingSetting.Enable {
		profiling()
	}

	//go metric
	if config.SplitterConfig.MetricSetting.Enable {
		metrics()
	}

	splitterServer, err := splitter.New()
	if err != nil {
		log.Error("splitter: create server error")
		log.DetailError(err)
		os.Exit(1)
	}

	splitterServer.Run()
}
func (w *serviceWrapper) Stop(s svc.Service) error {
	// Stop should not block. Return with a few seconds.
	return nil
}

func main() {
	svcConfig := &svc.Config{
		Name:        "bds-splitter",
		DisplayName: "BDS Splitter Application",
		Description: "",
		Arguments:   []string{"-c", linuxDefaultConfigFile},
	}
	wrapper := &serviceWrapper{}
	s, err := svc.New(wrapper, svcConfig)
	if err != nil {
		fmt.Print(err)
		os.Exit(2)
	}

	if len(os.Args) > 1 {
		for _, v := range svc.ControlAction {
			if os.Args[1] == v {
				err = svc.Control(s, os.Args[1])
				if err != nil {
					fmt.Println(err)
				}
				return
			}
		}
	}
	err = s.Run()
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
}

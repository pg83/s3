package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	logLevel   = new(slog.LevelVar)
	beforeExit = func() {}
)

func flags(name string) (*flag.FlagSet, *bool) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)

	return fs, fs.Bool("debug", false, "log the timing of every tick, flush, put and get")
}

func metricsFlag(fs *flag.FlagSet) *string {
	return fs.String("metrics", "", "address to serve Prometheus metrics on, e.g. 127.0.0.1:9100")
}

func parse(fs *flag.FlagSet, debug *bool) {
	throw(fs.Parse(os.Args[2:]))

	if *debug {
		logLevel.Set(slog.LevelDebug)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	try(func() {
		armChaos()
		exitOnSignal()

		go collectMetrics()

		switch os.Args[1] {
		case "cell":
			fs, debug := flags("cell")
			listen := []string{}

			fs.Func("listen", "address to serve cell requests on, may repeat", func(addr string) error {
				listen = append(listen, addr)

				return nil
			})

			load := fs.String("load", "", "directory on the SSD for blocks being read")
			store := fs.String("store", "", "directory on the SSD for blocks being written")
			hdd := fs.String("hdd", "", "raw block device for the log")
			metrics := metricsFlag(fs)

			parse(fs, debug)
			runCell(listen, *load, *store, *hdd, *metrics)
		case "front":
			fs, debug := flags("front")
			config := fs.String("c", "", "config file")
			listen := []string{}

			fs.Func("listen", "address to serve S3 on, may repeat", func(addr string) error {
				listen = append(listen, addr)

				return nil
			})

			metrics := metricsFlag(fs)

			parse(fs, debug)
			runFront(loadConfig(*config), listen, *metrics)
		case "scan":
			fs, debug := flags("scan")
			config := fs.String("c", "", "config file")
			host := fs.String("host", "", "name of this host in the config")
			age := fs.Duration("age", 10*time.Minute, "how long a put may wait for its third pieces before its host owes them")

			parse(fs, debug)
			runScan(loadConfig(*config), *host, *age)
		case "repair":
			fs, debug := flags("repair")
			config := fs.String("c", "", "config file")
			host := fs.String("host", "", "name of this host in the config")
			metrics := metricsFlag(fs)

			parse(fs, debug)
			runRepair(loadConfig(*config), *host, *metrics)
		case "nbd":
			fs, debug := flags("nbd")
			endpoint := fs.String("endpoint", "", "S3 endpoint of a front, e.g. http://127.0.0.1:8093")
			bucket := fs.String("bucket", "", "bucket holding the disk")
			disk := fs.String("disk", "", "name of the disk, the prefix of its keys and its export name")
			socket := fs.String("socket", "", "unix socket to serve NBD on")
			tick := fs.Duration("tick", 10*time.Second, "how often what was written is committed to the bucket")
			baseEvery := fs.Int("base-every", 64, "commits between two full maps of the disk")
			metrics := metricsFlag(fs)
			sizes := map[string]int64{"size": 0, "cache": 1 << 30, "dirty": 4 << 30}

			for name, usage := range map[string]string{
				"size":  "size of the disk, e.g. 4T",
				"cache": "clean blocks kept in memory, e.g. 1G",
				"dirty": "written blocks in memory before writes wait for a commit, e.g. 4G",
			} {
				fs.Func(name, usage, func(text string) error {
					v, err := parseSize(text)

					sizes[name] = v

					return err
				})
			}

			parse(fs, debug)
			runNbd(*endpoint, *bucket, *disk, sizes["size"], *socket, *tick, sizes["cache"], sizes["dirty"], *baseEvery, *metrics)
		case "web":
			fs, debug := flags("web")
			config := fs.String("c", "", "config file")
			listen := fs.String("listen", "", "address to serve the browser on")

			parse(fs, debug)
			runWeb(loadConfig(*config), *listen)
		default:
			printUsage()
			os.Exit(1)
		}
	}).catch(func(exc *Exception) {
		slog.Error(exc.error())
		os.Exit(1)
	})
}

func exitOnSignal() {
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		beforeExit()
		os.Exit(0)
	}()
}

func printUsage() {
	os.Stderr.WriteString(`Usage: s3 command [flags]

Commands:
  cell -listen addr [-listen addr] -load dir -store dir -hdd device [-metrics addr]
                                              append-only log on one disk
  front -c config.json -listen addr [-listen addr] [-metrics addr]
                                              S3 API over the cells and etcd
  repair -c config.json -host name [-metrics addr]
                                              rebuild the pieces this host owes
  scan -c config.json -host name [-age 10m]   hand long unsettled puts to this host's repair
  nbd -endpoint url -bucket name -disk name -size 4T -socket path [-tick 10s] [-cache 1G] [-dirty 4G] [-metrics addr]
                                              a block device over a bucket, served as NBD
  web -c config.json -listen addr             browse buckets and objects
`)
}

package main

import (
	"flag"
	"log/slog"
	"os"
)

var logLevel = new(slog.LevelVar)

func flags(name string) (*flag.FlagSet, *bool) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)

	return fs, fs.Bool("debug", false, "log the timing of every tick, flush, put and get")
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

			parse(fs, debug)
			runCell(listen, *load, *store, *hdd)
		case "front":
			fs, debug := flags("front")
			config := fs.String("c", "", "config file")
			listen := fs.String("listen", "", "address to serve S3 on")

			parse(fs, debug)
			runFront(loadConfig(*config), *listen)
		case "repair":
			fs, debug := flags("repair")
			config := fs.String("c", "", "config file")
			host := fs.String("host", "", "name of this host in the config")

			parse(fs, debug)
			runRepair(loadConfig(*config), *host)
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

func printUsage() {
	os.Stderr.WriteString(`Usage: s3 command [flags]

Commands:
  cell -listen addr [-listen addr] -load dir -store dir -hdd device
                                              append-only log on one disk
  front -c config.json -listen addr           S3 API over the cells and etcd
  repair -c config.json -host name            rebuild the pieces this host owes
  web -c config.json -listen addr             browse buckets and objects
`)
}

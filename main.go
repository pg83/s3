package main

import (
	"flag"
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	try(func() {
		switch os.Args[1] {
		case "cell":
			fs := flag.NewFlagSet("cell", flag.ExitOnError)
			listen := fs.String("listen", "", "address to serve cell requests on")
			load := fs.String("load", "", "directory on the SSD for blocks being read")
			store := fs.String("store", "", "directory on the SSD for blocks being written")
			hdd := fs.String("hdd", "", "raw block device for the log")

			throw(fs.Parse(os.Args[2:]))
			runCell(*listen, *load, *store, *hdd)
		case "front":
			fs := flag.NewFlagSet("front", flag.ExitOnError)
			config := fs.String("c", "", "config file")
			listen := fs.String("listen", "", "address to serve S3 on")

			throw(fs.Parse(os.Args[2:]))
			runFront(loadConfig(*config), *listen)
		case "repair":
			fs := flag.NewFlagSet("repair", flag.ExitOnError)
			config := fs.String("c", "", "config file")
			host := fs.String("host", "", "name of this host in the config")

			throw(fs.Parse(os.Args[2:]))
			runRepair(loadConfig(*config), *host)
		case "web":
			fs := flag.NewFlagSet("web", flag.ExitOnError)
			config := fs.String("c", "", "config file")
			listen := fs.String("listen", "", "address to serve the browser on")

			throw(fs.Parse(os.Args[2:]))
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
  cell -listen addr -load dir -store dir -hdd device
                                              append-only log on one disk
  front -c config.json -listen addr           S3 API over the cells and etcd
  repair -c config.json -host name            rebuild the pieces this host owes
  web -c config.json -listen addr             browse buckets and objects
`)
}

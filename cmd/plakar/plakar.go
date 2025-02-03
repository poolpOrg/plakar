package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/caching"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/denisbrodbeck/machineid"
	"github.com/google/uuid"

	_ "github.com/PlakarKorp/plakar/storage/backends/database"
	_ "github.com/PlakarKorp/plakar/storage/backends/fs"
	_ "github.com/PlakarKorp/plakar/storage/backends/http"
	_ "github.com/PlakarKorp/plakar/storage/backends/null"
	_ "github.com/PlakarKorp/plakar/storage/backends/plakard"
	_ "github.com/PlakarKorp/plakar/storage/backends/s3"

	_ "github.com/PlakarKorp/plakar/snapshot/importer/fs"
	_ "github.com/PlakarKorp/plakar/snapshot/importer/ftp"
	_ "github.com/PlakarKorp/plakar/snapshot/importer/s3"

	_ "github.com/PlakarKorp/plakar/snapshot/exporter/fs"
	_ "github.com/PlakarKorp/plakar/snapshot/exporter/s3"

	_ "github.com/PlakarKorp/plakar/classifier/backend/noop"
)

func main() {
	os.Exit(entryPoint())
}

func entryPoint() int {
	// default values
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}
	cwd, err = utils.NormalizePath(cwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	opt_cpuDefault := runtime.GOMAXPROCS(0)
	if opt_cpuDefault != 1 {
		opt_cpuDefault = opt_cpuDefault - 1
	}

	opt_userDefault, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: go away casper !\n", flag.CommandLine.Name())
		return 1
	}

	opt_hostnameDefault, err := os.Hostname()
	if err != nil {
		opt_hostnameDefault = "localhost"
	}

	opt_machineIdDefault, err := machineid.ID()
	if err != nil {
		opt_machineIdDefault = uuid.NewSHA1(uuid.Nil, []byte(opt_hostnameDefault)).String()
	}
	opt_machineIdDefault = strings.ToLower(opt_machineIdDefault)

	opt_usernameDefault := opt_userDefault.Username
	opt_configDefault := path.Join(opt_userDefault.HomeDir, ".plakarconfig")

	// command line overrides
	var opt_cpuCount int
	var opt_configfile string
	var opt_username string
	var opt_hostname string
	var opt_cpuProfile string
	var opt_memProfile string
	var opt_time bool
	var opt_trace string
	var opt_quiet bool
	var opt_keyfile string
	var opt_agentless bool

	flag.StringVar(&opt_configfile, "config", opt_configDefault, "configuration file")
	flag.IntVar(&opt_cpuCount, "cpu", opt_cpuDefault, "limit the number of usable cores")
	flag.StringVar(&opt_username, "username", opt_usernameDefault, "default username")
	flag.StringVar(&opt_hostname, "hostname", opt_hostnameDefault, "default hostname")
	flag.StringVar(&opt_cpuProfile, "profile-cpu", "", "profile CPU usage")
	flag.StringVar(&opt_memProfile, "profile-mem", "", "profile MEM usage")
	flag.BoolVar(&opt_time, "time", false, "display command execution time")
	flag.StringVar(&opt_trace, "trace", "", "display trace logs")
	flag.BoolVar(&opt_quiet, "quiet", false, "no output except errors")
	flag.StringVar(&opt_keyfile, "keyfile", "", "use passphrase from key file when prompted")
	flag.BoolVar(&opt_agentless, "no-agent", false, "run without agent")
	flag.Parse()

	ctx := appcontext.NewAppContext()
	defer ctx.Close()

	ctx.Client = "plakar/" + utils.GetVersion()
	ctx.CWD = cwd
	ctx.KeyringDir = filepath.Join(opt_userDefault.HomeDir, ".plakar-keyring")

	cacheSubDir := "plakar"
	if opt_agentless {
		cacheSubDir = "plakar-agentless"
	}
	cacheDir, err := utils.GetCacheDir(cacheSubDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get cache directory: %s\n", flag.CommandLine.Name(), err)
		return 1
	}
	ctx.CacheDir = cacheDir
	ctx.SetCache(caching.NewManager(cacheDir))
	defer ctx.GetCache().Close()

	// best effort check if security or reliability fix have been issued
	if rus, err := utils.CheckUpdate(ctx.CacheDir); err == nil {
		if rus.SecurityFix || rus.ReliabilityFix {
			concerns := ""
			if rus.SecurityFix {
				concerns = "security"
			}
			if rus.ReliabilityFix {
				if concerns != "" {
					concerns += " and "
				}
				concerns += "reliability"
			}
			fmt.Fprintf(os.Stderr, "WARNING: %s concerns affect your current version, please upgrade to %s (+%d releases).\n", concerns, rus.Latest, rus.FoundCount)
		}
	}

	// setup from default + override
	if opt_cpuCount > runtime.NumCPU() {
		fmt.Fprintf(os.Stderr, "%s: can't use more cores than available: %d\n", flag.CommandLine.Name(), runtime.NumCPU())
		return 1
	}
	runtime.GOMAXPROCS(opt_cpuCount)

	if opt_cpuProfile != "" {
		f, err := os.Create(opt_cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not create CPU profile: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not start CPU profile: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}

	var secretFromKeyfile string
	if opt_keyfile != "" {
		data, err := os.ReadFile(opt_keyfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not read key file: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		secretFromKeyfile = strings.TrimSuffix(string(data), "\n")
	}

	ctx.OperatingSystem = runtime.GOOS
	ctx.Architecture = runtime.GOARCH
	ctx.NumCPU = opt_cpuCount
	ctx.Username = opt_username
	ctx.Hostname = opt_hostname
	ctx.CommandLine = strings.Join(os.Args, " ")
	ctx.MachineID = opt_machineIdDefault
	ctx.KeyFromFile = secretFromKeyfile
	ctx.HomeDir = opt_userDefault.HomeDir
	ctx.ProcessID = os.Getpid()
	ctx.MaxConcurrency = ctx.NumCPU*8 + 1

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: a subcommand must be provided\n", filepath.Base(flag.CommandLine.Name()))
		items := append(make([]string, 0, len(subcommands.List())), subcommands.List()...)
		sort.Strings(items)
		for _, k := range items {
			fmt.Fprintf(os.Stderr, "  %s\n", k)
		}

		return 1
	}

	logger := logging.NewLogger(os.Stdout, os.Stderr)

	// start logging
	if !opt_quiet {
		logger.EnableInfo()
	}
	if opt_trace != "" {
		logger.EnableTrace(opt_trace)
	}

	ctx.SetLogger(logger)

	command, args := flag.Args()[0], flag.Args()[1:]

	var repositoryPath string
	if flag.Arg(0) == "on" {
		if len(flag.Args()) < 2 {
			log.Fatalf("%s: missing plakar repository", flag.CommandLine.Name())
		}
		if len(flag.Args()) < 3 {
			log.Fatalf("%s: missing command", flag.CommandLine.Name())
		}
		repositoryPath = flag.Arg(1)
		command, args = flag.Arg(2), flag.Args()[3:]
	} else {
		repositoryPath = os.Getenv("PLAKAR_REPOSITORY")
		if repositoryPath == "" {
			repositoryPath = filepath.Join(ctx.HomeDir, ".plakar")
		}
	}

	// these commands need to be ran before the repository is opened
	if command == "agent" || command == "create" || command == "version" || command == "stdio" || command == "help" || command == "id" {
		retval, err := subcommands.Execute(ctx, nil, command, args, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
			return retval
		}
		return retval
	}

	// special case, server skips passphrase as it only serves storage layer
	skipPassphrase := false
	if command == "server" {
		opt_agentless = true
		skipPassphrase = true
	}

	store, err := storage.Open(repositoryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return 1
	}

	if store.Configuration().Version != storage.VERSION {
		fmt.Fprintf(os.Stderr, "%s: incompatible repository version: %s != %s\n",
			flag.CommandLine.Name(), store.Configuration().Version, storage.VERSION)
		return 1
	}

	var secret []byte
	if !skipPassphrase {
		if store.Configuration().Encryption != nil {
			derived := false
			envPassphrase := os.Getenv("PLAKAR_PASSPHRASE")
			if ctx.KeyFromFile == "" {
				for attempts := 0; attempts < 3; attempts++ {
					var passphrase []byte
					if envPassphrase == "" {
						passphrase, err = utils.GetPassphrase("repository")
						if err != nil {
							break
						}
					} else {
						passphrase = []byte(envPassphrase)
					}

					key, err := encryption.DeriveKey(store.Configuration().Encryption.KDFParams, passphrase)
					if err != nil {
						continue
					}
					if !encryption.VerifyCanary(key, store.Configuration().Encryption.Canary) {
						if envPassphrase != "" {
							break
						}
						continue
					}
					derived = true
					break
				}
			} else {
				key, err := encryption.DeriveKey(store.Configuration().Encryption.KDFParams, []byte(ctx.KeyFromFile))
				if err == nil {
					if encryption.VerifyCanary(key, store.Configuration().Encryption.Canary) {
						derived = true
					}
				}
			}
			if !derived {
				fmt.Fprintf(os.Stderr, "%s: could not derive secret\n", flag.CommandLine.Name())
				os.Exit(1)
			}
			ctx.SetSecret(secret)
		}
	}

	var repo *repository.Repository
	if opt_agentless && command != "server" {
		repo, err = repository.New(ctx, store, secret)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
	} else {
		repo, err = repository.NewNoRebuild(ctx, store, secret)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
	}

	// commands below all operate on an open repository
	t0 := time.Now()
	status, err := subcommands.Execute(ctx, repo, command, args, opt_agentless)
	t1 := time.Since(t0)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
	}

	err = repo.Close()
	if err != nil {
		logger.Warn("could not close repository: %s", err)
	}

	err = store.Close()
	if err != nil {
		logger.Warn("could not close repository: %s", err)
	}

	ctx.Close()

	if opt_time {
		fmt.Println("time:", t1)
	}

	if opt_memProfile != "" {
		f, err := os.Create(opt_memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not write MEM profile: %d\n", flag.CommandLine.Name(), err)
			return 1
		}
	}

	return status
}

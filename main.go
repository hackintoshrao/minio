/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"

	"github.com/davecheney/profile"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/probe"
	"github.com/olekukonko/ts"
)

var (
	// global flags for minio.
	minioFlags = []cli.Flag{
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "Show help.",
		},
	}
)

// Help template for minio.
var minioHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

DESCRIPTION:
  {{.Description}}

USAGE:
  minio {{if .Flags}}[flags] {{end}}command{{if .Flags}}{{end}} [arguments...]

COMMANDS:
  {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
  {{end}}{{if .Flags}}
FLAGS:
  {{range .Flags}}{{.}}
  {{end}}{{end}}
VERSION:
  ` + minioVersion +
	`{{ "\n"}}{{range $key, $value := ExtraInfo}}
{{$key}}:
  {{$value}}
{{end}}`

// init - check the environment before main starts
func init() {
	// Check if minio was compiled using a supported version of Golang.
	checkGoVersion()

	// It is an unsafe practice to run network services as
	// root. Containers are an exception.
	if !isContainerized() && os.Geteuid() == 0 {
		console.Fatalln("Please run ‘minio’ as a non-root user.")
	}
}

func migrate() {
	// Migrate config file
	migrateConfig()

	// Migrate other configs here.
}

func enableLoggers() {
	// Enable all loggers here.
	enableConsoleLogger()
	enableFileLogger()

	// Add your logger here.
}

// Tries to get os/arch/platform specific information
// Returns a map of current os/arch/platform/memstats
func getSystemData() map[string]string {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	mem := fmt.Sprintf("Used: %s | Allocated: %s | Used-Heap: %s | Allocated-Heap: %s",
		humanize.Bytes(memstats.Alloc),
		humanize.Bytes(memstats.TotalAlloc),
		humanize.Bytes(memstats.HeapAlloc),
		humanize.Bytes(memstats.HeapSys))
	platform := fmt.Sprintf("Host: %s | OS: %s | Arch: %s",
		host,
		runtime.GOOS,
		runtime.GOARCH)
	goruntime := fmt.Sprintf("Version: %s | CPUs: %s", runtime.Version(), strconv.Itoa(runtime.NumCPU()))
	return map[string]string{
		"PLATFORM": platform,
		"RUNTIME":  goruntime,
		"MEM":      mem,
	}
}

func findClosestCommands(command string) []string {
	var closestCommands []string
	for _, value := range commandsTree.PrefixMatch(command) {
		closestCommands = append(closestCommands, value.(string))
	}
	sort.Strings(closestCommands)
	// Suggest other close commands - allow missed, wrongly added and
	// even transposed characters
	for _, value := range commandsTree.walk(commandsTree.root) {
		if sort.SearchStrings(closestCommands, value.(string)) < len(closestCommands) {
			continue
		}
		// 2 is arbitrary and represents the max
		// allowed number of typed errors
		if DamerauLevenshteinDistance(command, value.(string)) < 2 {
			closestCommands = append(closestCommands, value.(string))
		}
	}
	return closestCommands
}

func registerApp() *cli.App {
	// Register all commands.
	registerCommand(serverCmd)
	registerCommand(versionCmd)
	registerCommand(updateCmd)

	// Set up app.
	app := cli.NewApp()
	app.Name = "Minio"
	app.Author = "Minio.io"
	app.Usage = "Distributed Object Storage Server for Micro Services."
	app.Description = `Micro services environment provisions one Minio server per application instance. Scalability is achieved through large number of smaller personalized instances. This version of the Minio binary is built using Filesystem storage backend for magnetic and solid state disks.`
	app.Flags = append(minioFlags, globalFlags...)
	app.Commands = commands
	app.CustomAppHelpTemplate = minioHelpTemplate
	app.CommandNotFound = func(ctx *cli.Context, command string) {
		msg := fmt.Sprintf("‘%s’ is not a minio sub-command. See ‘minio --help’.", command)
		closestCommands := findClosestCommands(command)
		if len(closestCommands) > 0 {
			msg += fmt.Sprintf("\n\nDid you mean one of these?\n")
			for _, cmd := range closestCommands {
				msg += fmt.Sprintf("        ‘%s’\n", cmd)
			}
		}
		console.Fatalln(msg)
	}
	return app
}

func checkMainSyntax(c *cli.Context) {
	configPath, err := getConfigPath()
	if err != nil {
		console.Fatalf("Unable to obtain user's home directory. \nError: %s\n", err)
	}
	if configPath == "" {
		console.Fatalln("Config folder cannot be empty, please specify --config-dir <foldername>.")
	}
}

func main() {

	// configuring the profiler.
	cfg := profile.Config{
		CPUProfile: true,
		// NoShutdownHook: true, // do not hook SIGINT
	}

	// starting the profiler.
	p := profile.Start(&cfg)
	defer p.Stop()

	probe.Init() // Set project's root source path.
	probe.SetAppInfo("Release-Tag", minioReleaseTag)
	probe.SetAppInfo("Commit-ID", minioShortCommitID)

	app := registerApp()
	app.Before = func(c *cli.Context) error {
		// Set global flags.
		setGlobalsFromContext(c)

		// Sets new config folder.
		setGlobalConfigPath(c.GlobalString("config-dir"))

		// Valid input arguments to main.
		checkMainSyntax(c)

		// Migrate any old version of config / state files to newer format.
		migrate()

		// Initialize config.
		err := initConfig()
		fatalIf(err, "Unable to initialize minio config.", nil)

		// Enable all loggers by now.
		enableLoggers()

		// Initialize name space lock.
		initNSLock()

		// Do not print update messages, if quiet flag is set.
		if !globalQuiet {
			// Do not print any errors in release update function.
			noError := true
			updateMsg := getReleaseUpdate(minioUpdateStableURL, noError)
			if updateMsg.Update {
				console.Println(updateMsg)
			}
		}

		// Return here.
		return nil
	}
	app.ExtraInfo = func() map[string]string {
		if _, e := ts.GetSize(); e != nil {
			globalQuiet = true
		}
		// Enable if debug is enabled.
		if globalDebug {
			return getSystemData()
		}
		return make(map[string]string)
	}
	// Run the app - exit on error.
	app.RunAndExitOnError()
}

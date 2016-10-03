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

package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/objcache"
)

var srvConfig serverCmdConfig

var serverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: "Specify custom server \"ADDRESS:PORT\", defaults to \":9000\".",
	},
	cli.StringFlag{
		Name:  "ignore-disks",
		Usage: "Specify comma separated list of disks that are offline.",
	},
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "Start object storage server.",
	Flags:  append(serverFlags, globalFlags...),
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  minio {{.Name}} - {{.Usage}}

USAGE:
  minio {{.Name}} [FLAGS] PATH [PATH...]

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}
ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Access key string of 5 to 20 characters in length.
     MINIO_SECRET_KEY: Secret key string of 8 to 40 characters in length.

  CACHING:
     MINIO_CACHE_SIZE: Set total cache size in NN[GB|MB|KB]. Defaults to 8GB.
     MINIO_CACHE_EXPIRY: Set cache expiration duration in NN[h|m|s]. Defaults to 72 hours.

EXAMPLES:
  1. Start minio server.
      $ minio {{.Name}} /home/shared

  2. Start minio server bound to a specific IP:PORT, when you have multiple network interfaces.
      $ minio {{.Name}} --address 192.168.1.101:9000 /home/shared

  3. Start minio server on Windows.
      $ minio {{.Name}} C:\MyShare

  4. Start minio server on 12 disks to enable erasure coded layer with 6 data and 6 parity.
      $ minio {{.Name}} /mnt/export1/ /mnt/export2/ /mnt/export3/ /mnt/export4/ \
          /mnt/export5/ /mnt/export6/ /mnt/export7/ /mnt/export8/ /mnt/export9/ \
          /mnt/export10/ /mnt/export11/ /mnt/export12/

  5. Start minio server on 12 disks while ignoring two disks for initialization.
      $ minio {{.Name}} --ignore-disks=/mnt/export1/ /mnt/export1/ /mnt/export2/ \
          /mnt/export3/ /mnt/export4/ /mnt/export5/ /mnt/export6/ /mnt/export7/ \
	  /mnt/export8/ /mnt/export9/ /mnt/export10/ /mnt/export11/ /mnt/export12/

  6. Start minio server on a 4 node distributed setup. Type the following command on all the 4 nodes.
      $ export MINIO_ACCESS_KEY=minio
      $ export MINIO_SECRET_KEY=miniostorage
      $ minio {{.Name}} 192.168.1.11:/mnt/export/ 192.168.1.12:/mnt/export/ \
          192.168.1.13:/mnt/export/ 192.168.1.14:/mnt/export/

`,
}

type Context struct {
	// mutex for atomic access of fields.
	sync.Mutex
	// indicates whether the connection in http or https.
	IsSecure       bool
	IsDebugEnabled bool
	// Trace flag set via environment setting.
	IsTraceEnabled bool
	// Debug flag set to print debug info.
	IsDebug bool
	// Directory in which Minio configuration is stored.
	ConfigDir string
	// server address.
	ServerAddr string
	// Cache size.
	CacheSize int64
	// Cache Duration.
	CacheDuration time.Duration
	// Disks.
	Disks []string
	// Ignored Disks.
	IgnoredDisks []string
	// Maximum connections handled per
	// server, defaults to 0 (unlimited).
	MaxConn int
}

func (ctx *Context) GetConfigDir() string {
	ctx.Lock()
	defer ctx.UnLock()
	return ctx.ConfigDir
}

func NewContext() *Context {

	// fetch from environment variables and set the global values related to locks.

	return &Context{

		IsSecure:      false,
		CacheSize:     uint64(defaultCacheSize),
		CacheDuration: objcache.DefaultExpiry,
		MaxConn:       0,
	}

}

func setGlobalsDebugFromEnv() bool {
	debugEnv := os.Getenv("MINIO_DEBUG")
	switch debugEnv {
	case "lock":
		globalDebugLock = true
	case "mem":
		globalDebugMemory = true
	}
	return globalDebugLock || globalDebugMemory
}

type Server struct {
	// Mutex for atomic access of the struct fields.
	sync.Mutex
	// Object Layer for FS operations.
	// Object Layer can be either FS or XL.
	ObjectLayer ObjectLayer
	// http handler with endpoints registered.
	Mux *http.Handler
	// InMemory lock used for FS operations.
	NsMutex *nsLockMap
	// All configuration values.
	Context          *Context
	ShutdownSignalCh chan shutdownSignal
	// server config.
	ServerConfig *serverConfigV7
	// profiler interface.
	Profiler profiler
}

type serverCmdConfig struct {
	serverAddr   string
	disks        []string
	ignoredDisks []string
}

// getListenIPs - gets all the ips to listen on.
func getListenIPs(httpServerConf *http.Server) (hosts []string, port string) {
	host, port, err := net.SplitHostPort(httpServerConf.Addr)
	fatalIf(err, "Unable to parse host address.", httpServerConf.Addr)

	if host != "" {
		hosts = append(hosts, host)
		return hosts, port
	}
	addrs, err := net.InterfaceAddrs()
	fatalIf(err, "Unable to determine network interface address.")
	for _, addr := range addrs {
		if addr.Network() == "ip+net" {
			host := strings.Split(addr.String(), "/")[0]
			if ip := net.ParseIP(host); ip.To4() != nil {
				hosts = append(hosts, host)
			}
		}
	}
	return hosts, port
}

// Finalizes the endpoints based on the host list and port.
func finalizeEndpoints(tls bool, apiServer *http.Server) (endPoints []string) {
	// Get list of listen ips and port.
	hosts, port := getListenIPs(apiServer)

	// Verify current scheme.
	scheme := "http"
	if tls {
		scheme = "https"
	}

	ips := getIPsFromHosts(hosts)

	// Construct proper endpoints.
	for _, ip := range ips {
		endPoints = append(endPoints, fmt.Sprintf("%s://%s:%s", scheme, ip.String(), port))
	}

	// Success.
	return endPoints
}

// increase the limits the for the max open files and memory allocation fpr a processs.
func setServerLimits(c *cli.Context, ctx *Context) {
	// Set maxOpenFiles, This is necessary since default operating
	// system limits of 1024, 2048 are not enough for Minio server.
	setMaxOpenFiles()
	// Set maxMemory, This is necessary since default operating
	// system limits might be changed and we need to make sure we
	// do not crash the server so the set the maxCacheSize appropriately.
	setMaxMemory()

	// Do not fail if this is not allowed, lower limits are fine as well.
}

// obtain cache duration from ENV, if not set return the default expiry.
func getCacheDuration() (time.Duration, error) {

	// Fetch cache expiry from environment variable.
	if cacheExpiryStr := os.Getenv("MINIO_CACHE_EXPIRY"); cacheExpiryStr != "" {
		// We need to parse cache expiry to its time.Duration value.
		return time.ParseDuration(cacheExpiryStr)
	}
	return objcache.DefaultExpiry, nil
}

// obtain cache size from environment variable, return default cache size if not set.
func getCacheSize() (uint64, err) {
	// Fetch max cache size from environment variable.
	if maxCacheSizeStr := os.Getenv("MINIO_CACHE_SIZE"); maxCacheSizeStr != "" {
		// We need to parse cache size to its integer value.
		return strconvBytes(maxCacheSizeStr)
	}
	return uint64(defaultCacheSize), nil
}

func getMaxConn() (int, err) {

	// Fetch max conn limit from environment variable.
	if maxConnStr := os.Getenv("MINIO_MAXCONN"); maxConnStr != "" {
		// We need to parse to its integer value.
		return strconv.Atoi(maxConnStr)
	}
	return 0, nil
}

// Validate if input disks are sufficient for initializing XL.
func checkSufficientDisks(disks []string) error {
	// Verify total number of disks.
	totalDisks := len(disks)
	if totalDisks > maxErasureBlocks {
		return errXLMaxDisks
	}
	if totalDisks < minErasureBlocks {
		return errXLMinDisks
	}

	// isEven function to verify if a given number if even.
	isEven := func(number int) bool {
		return number%2 == 0
	}

	// Verify if we have even number of disks.
	// only combination of 4, 6, 8, 10, 12, 14, 16 are supported.
	if !isEven(totalDisks) {
		return errXLNumDisks
	}

	// Success.
	return nil
}

// Validates if disks are of supported format, invalid arguments are rejected.
func checkNamingDisks(disks []string) error {
	for _, disk := range disks {
		_, _, err := splitNetPath(disk)
		if err != nil {
			return err
		}
	}
	return nil
}

// Check server arguments.
func checkServerSyntax(c *cli.Context) {
	if !c.Args().Present() || c.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(c, "server", 1)
	}
	disks := c.Args()
	if len(disks) > 1 {
		// Validate if input disks have duplicates in them.
		err := checkDuplicates(disks)
		fatalIf(err, "Invalid disk arguments for server.")

		// Validate if input disks are sufficient for erasure coded setup.
		err = checkSufficientDisks(disks)
		fatalIf(err, "Invalid disk arguments for server.")

		// Validate if input disks are properly named in accordance with either
		//  - /mnt/disk1
		//  - ip:/mnt/disk1
		err = checkNamingDisks(disks)
		fatalIf(err, "Invalid disk arguments for server.")
	}
}

// Extract port number from address address should be of the form host:port.
func getPort(address string) int {
	_, portStr, _ := net.SplitHostPort(address)

	// If port empty, default to port '80'
	if portStr == "" {
		portStr = "80"
		// if SSL is enabled, choose port as "443" instead.
		if isSSL() {
			portStr = "443"
		}
	}

	// Return converted port number.
	portInt, err := strconv.Atoi(portStr)
	fatalIf(err, "Invalid port number.")
	return portInt
}

// Returns if slice of disks is a distributed setup.
func isDistributedSetup(disks []string) (isDist bool) {
	// Port to connect to for the lock servers in a distributed setup.
	for _, disk := range disks {
		if !isLocalStorage(disk) {
			// One or more disks supplied as arguments are not
			// attached to the local node.
			isDist = true
		}
	}
	return isDist
}

// Format disks before initialization object layer.
func formatDisks(disks, ignoredDisks []string) error {
	storageDisks, err := waitForFormattingDisks(disks, ignoredDisks)
	for _, storage := range storageDisks {
		if storage == nil {
			continue
		}
		switch store := storage.(type) {
		// Closing associated TCP connections since
		// []StorageAPI is garbage collected eventually.
		case networkStorage:
			store.rpcClient.Close()
		}
	}
	if err != nil {
		return err
	}
	if isLocalStorage(disks[0]) {
		// notify every one else that they can try init again.
		for _, storage := range storageDisks {
			switch store := storage.(type) {
			// Closing associated TCP connections since
			// []StorageAPI is garbage collected
			// eventually.
			case networkStorage:
				var reply GenericReply
				_ = store.rpcClient.Call("Storage.TryInitHandler", &GenericArgs{}, &reply)
			}
		}
	}
	return nil
}

func getConfigDir(c *cli.Context) (string, error) {
	return getConfigPath(c.GlobalString("config-dir"))
}
func initServer(c *cli.Context) {
}

// serverMain handler called for 'minio server' command.
func serverMain(c *cli.Context) {

	// obtain new context to store all the important settings of the server.
	context := NewContext()

	context.Lock()
	defer context.Unlock()
	// get minio config dir.
	configDir, err := getConfigDir(c)
	// abort on error.
	fatalIf(err, "Unable to get minio config dir.")
	context.ConfigDir = configDir

	// Migrate any old version of config / state files to newer format.
	migrate()

	serverCtx := &Server{}
	serverCtx.Lock()
	defer serverCtx.Unlock()
	// Initialize config.
	config, err := initConfig(context)
	fatalIf(err, "Unable to initialize minio config.")

	serverCtx.ServerConfig = config

	// Enable all loggers by now.
	enableLoggers(config)

	// Init the error tracing module.
	initError()

	// Set quiet flag.
	isQuiet := c.Bool("quiet") || c.GlobalBool("quiet")

	// Do not print update messages, if quiet flag is set.
	if !isQuiet {
		if strings.HasPrefix(Version, "RELEASE.") {
			updateMsg, _, err := getReleaseUpdate(minioUpdateStableURL)
			if err != nil {
				// Ignore any errors during getReleaseUpdate() because
				// the internet might not be available.
				return nil
			}
			console.Println(updateMsg)
		}
	}

	// Start profiler if env is set.
	if profile := os.Getenv("MINIO_PROFILER"); profiler != "" {
		serverCtx.Profiler = startProfiler(profiler)
	}
	// Check 'server' cli arguments.
	checkServerSyntax(c)
	// obtain the max connection.
	maxConn, err := getMaxConn()
	if err != nil {
		fatalIf(err, "Unable to convert MINIO_MAXCONN=%s environment variable into its integer value.", os.Getenv("MINIO_MAXCONN"))
	}
	// Set the `MaxConn` field in the context.
	context.MaxConn = maxConn

	// get cache size.
	cacheSize, err := getCacheSize()
	if err != nil {
		fatalIf(err, "Unable to convert MINIO_CACHE_SIZE=%s environment variable into its integer value.", os.Getenv("MINIO_CACHE_SIZE"))
	}
	// set the cache size in the context.
	context.CacheSize = cacheSize

	// get cache duration.
	cacheDuration, err := getCacheDuration()
	if err != nil {
		fatalIf(err, "Unable to convert MINIO_CACHE_EXPIRY=%s environment variable into its time.Duration value.", os.Getenv("MINIO_CACHE_EXPIRY"))
	}
	// set the cache duration in the context.
	context.CacheDuration = cacheDuration

	// Create certs path.
	err := createCertsPath()
	fatalIf(err, "Unable to create \"certs\" directory.")

	// Fetch access keys from environment variables if any and update the config.
	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")

	// Validate if both keys are specified and they are valid save them.
	if accessKey != "" && secretKey != "" {
		if !isValidAccessKey.MatchString(accessKey) {
			fatalIf(errInvalidArgument, "Invalid access key.")
		}
		if !isValidSecretKey.MatchString(secretKey) {
			fatalIf(errInvalidArgument, "Invalid secret key.")
		}
		// Set new credentials.
		serverCtx.ServerConfig.SetCredential(credential{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		})
		// Save new config.
		err = serverCtx.ServerConfig.Save()
		fatalIf(err, "Unable to save config.")
	}

	// increase the memoery and open file limits of the server.
	setServerLimits()

	// If https.
	tls := isSSL()

	context.IsSecure = tls
	// Server address.
	serverAddress := c.String("address")

	context.ServerAddr = serverAddress
	// Check if requested port is available.
	port := getPort(serverAddress)
	err := checkPortAvailability(port)
	fatalIf(err, "Port unavailable %d", port)

	// Disks to be ignored in server init, to skip format healing.
	ignoredDisks := strings.Split(c.String("ignore-disks"), ",")

	// Disks to be used in server init.
	disks := c.Args()

	isDist := isDistributedSetup(disks)
	// Set nodes for dsync for distributed setup.
	if isDist {
		err = initDsyncNodes(disks, port)
		fatalIf(err, "Unable to initialize distributed locking")
	}

	// Initialize name space lock.
	initNSLock(isDist)

	// Configure server.
	srvConfig = serverCmdConfig{
		serverAddr:   serverAddress,
		disks:        disks,
		ignoredDisks: ignoredDisks,
	}

	// Initialize and monitor shutdown signals.
	err = initGracefulShutdown(os.Exit)
	fatalIf(err, "Unable to initialize graceful shutdown operation")

	// Configure server.
	handler := configureServerHandler(srvConfig)

	apiServer := NewServerMux(serverAddress, handler)

	// Fetch endpoints which we are going to serve from.
	endPoints := finalizeEndpoints(tls, &apiServer.Server)

	// Register generic callbacks.
	globalShutdownCBs.AddGenericCB(func() errCode {
		// apiServer.Stop()
		return exitSuccess
	})

	// Start server.
	// Configure TLS if certs are available.
	wait := make(chan struct{}, 1)
	go func(tls bool, wait chan<- struct{}) {
		fatalIf(func() error {
			defer func() {
				wait <- struct{}{}
			}()
			if tls {
				return apiServer.ListenAndServeTLS(mustGetCertFile(), mustGetKeyFile())
			} // Fallback to http.
			return apiServer.ListenAndServe()
		}(), "Failed to start minio server.")
	}(tls, wait)

	// Wait for formatting of disks.
	err = formatDisks(disks, ignoredDisks)
	if err != nil {
		// FIXME: call graceful exit
		errorIf(err, "formatting storage disks failed")
		return
	}

	// Once formatted, initialize object layer.
	newObject, err := newObjectLayer(disks, ignoredDisks)
	if err != nil {
		// FIXME: call graceful exit
		errorIf(err, "intializing object layer failed")
		return
	}

	// Prints the formatted startup message.
	printStartupMessage(endPoints)

	objLayerMutex.Lock()
	globalObjectAPI = newObject
	objLayerMutex.Unlock()

	// Waits on the server.
	<-wait
}

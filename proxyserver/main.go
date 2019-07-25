package main

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"time"

	proxy "github.com/operasoftware/gocqlproxy"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type proxyConfig struct {
	ListenHost string `json:"listen_host"`
	LocalDatacenter string `json:"local_datacenter"`
	ScyllaNodes []string `json:"scylla_nodes"`
	Keyspace string `json:"keyspace"`
}

func handleConfigError(err error) {
	if err != nil {
		proxy.Logger.Println("gocqlproxy config read error:", err)
		os.Exit(1)
	}
}

func readConfig(filename string) proxyConfig {
	file, err := os.Open(filename)
	handleConfigError(err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	config := proxyConfig{}
	handleConfigError(decoder.Decode(&config))

	if config.ListenHost == "" {
		handleConfigError(errors.New("listen_host value missing"))
	}
	if config.LocalDatacenter == "" {
		handleConfigError(errors.New("local_datacenter value missing"))
	}
	if len(config.ScyllaNodes) == 0 {
		handleConfigError(errors.New("scylla_nodes value missing"))
	}
	if config.Keyspace == "" {
		handleConfigError(errors.New("keyspace value missing"))
	}

	return config
}

func main() {
	if len(os.Args) < 2 {
		proxy.Logger.Println("gocqlproxy error: missing config json filename argument")
		os.Exit(1)
	}
	// setup the basic cluster variables:
	config := readConfig(os.Args[1])
	cluster := proxy.NewCluster(config.ScyllaNodes...)
	cluster.Keyspace = config.Keyspace
	cluster.Compressor = &proxy.SnappyCompressor{}

	// set username/password if provided:
	if scyllaUsername, foundUsername := os.LookupEnv("SCYLLA_USERNAME"); foundUsername {
		authenticator := &proxy.PasswordAuthenticator{
			Username: scyllaUsername,
		}
		cluster.Authenticator = authenticator
		scyllaPassword, foundPassword := os.LookupEnv("SCYLLA_PASSWORD")
		if foundPassword {
			authenticator.Password = scyllaPassword
		}
	}

	// some Opera Sync-specific settings: (not generic, could be made configurable)
	cluster.Consistency = proxy.LocalQuorum
	cluster.Timeout = 10 * time.Second

	// enable token aware host selection policy, set a local DC for multi-DC setup:
	fallback := proxy.DCAwareRoundRobinPolicy(config.LocalDatacenter)
	hostSelectionPolicy := proxy.TokenAwareHostPolicy(fallback, proxy.ShuffleReplicas())
	cluster.PoolConfig.HostSelectionPolicy = hostSelectionPolicy

	// create a session:
	session, err := cluster.CreateSession()
	check(err)

	// set the partitioner:
	// (needs session to be up, otherwise policy.getKeyspaceName is unset,
	//  which causes a crash when getKeyspaceName is called from SetPartitioner)
	hostSelectionPolicy.SetPartitioner("Murmur3Partitioner")

	listenConnection, err := net.Listen("tcp", config.ListenHost)
	check(err)

	proxy.Logger.Println("gocqlproxy: listening on", config.ListenHost)
	maxConnections := 200
	semaphore := make(chan bool, maxConnections)
	for {
		var conn net.Conn
		select {
		case semaphore <- true:
			listenConnection.(*net.TCPListener).SetDeadline(time.Time{})
			conn, err = listenConnection.Accept()
		case <-time.After(time.Second * 1):
			// clean the queue if unable to accept after 1 second
			// (accept&drop all connections until there are no new ones for 100ms,
			//  or some other connection closes)
			for len(semaphore) == maxConnections {
				listenConnection.(*net.TCPListener).SetDeadline(time.Now().Add(time.Millisecond * 100))
				conn, err := listenConnection.Accept()
				if err != nil {
					proxy.Logger.Println("gocqlproxy accept failed:", err)
					break
				}
				conn.Close()
			}
			continue
		}
		conn, err := listenConnection.Accept()
		proxy.Logger.Println("gocqlproxy: new connection from " + conn.RemoteAddr().String())
		check(err)
		go func() {
			defer func() { <-semaphore }()
			go session.HandleConn(conn)
		}()
	}
}

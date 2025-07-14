package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/yugabyte-db/shopify-yb-client/pb"
	"github.com/Shopify/yugabyte-db/shopify-yb-client/pkg/rpc"
	"google.golang.org/protobuf/proto"
)

type TabletServer struct {
	UUID                 string    `json:"uuid"`
	SequenceNumber       int64     `json:"sequence_number"`
	RPCAddress           string    `json:"rpc_address"`
	PlacementCloud       string    `json:"placement_cloud,omitempty"`
	PlacementRegion      string    `json:"placement_region,omitempty"`
	PlacementZone        string    `json:"placement_zone,omitempty"`
	Alive                bool      `json:"alive"`
	MillisSinceHeartbeat int32     `json:"millis_since_heartbeat,omitempty"`
	LastHeartbeat        time.Time `json:"last_heartbeat"`
}

type TabletServersResponse struct {
	Servers []TabletServer `json:"servers"`
	Count   int            `json:"count"`
	Error   string         `json:"error,omitempty"`
}

type Master struct {
	UUID        string `json:"uuid"`
	RPCAddress  string `json:"rpc_address"`
	HTTPAddress string `json:"http_address,omitempty"`
	Role        string `json:"role"`
	Cloud       string `json:"cloud,omitempty"`
	Region      string `json:"region,omitempty"`
	Zone        string `json:"zone,omitempty"`
}

type MastersResponse struct {
	Masters []Master `json:"masters"`
	Count   int      `json:"count"`
	Error   string   `json:"error,omitempty"`
}

type ClusterConfig struct {
	Version           int                    `json:"version"`
	ClusterUUID       string                 `json:"cluster_uuid"`
	ServerVersion     string                 `json:"server_version,omitempty"`
	ReplicationInfo   map[string]interface{} `json:"replication_info,omitempty"`
	EncryptionEnabled bool                   `json:"encryption_enabled"`
}

type ClusterConfigResponse struct {
	Config ClusterConfig `json:"config"`
	Error  string        `json:"error,omitempty"`
}

var (
	masterAddr string
	listenAddr string
)

func main() {
	flag.StringVar(&masterAddr, "master", "localhost:7100", "YugabyteDB master address")
	flag.StringVar(&listenAddr, "listen", ":8080", "HTTP server listen address")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	http.HandleFunc("/api/v1/tablet-servers", handleTabletServers)
	http.HandleFunc("/api/v1/masters", handleListMasters)
	http.HandleFunc("/api/v1/cluster-config", handleClusterConfig)

	log.Printf("Starting HTTP server on %s", listenAddr)
	log.Printf("YugabyteDB master configured at %s", masterAddr)
	log.Println("Available endpoints:")
	log.Printf("  GET http://localhost%s/api/v1/tablet-servers", listenAddr)
	log.Printf("  GET http://localhost%s/api/v1/masters", listenAddr)
	log.Printf("  GET http://localhost%s/api/v1/cluster-config", listenAddr)

	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func handleTabletServers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := fetchTabletServers()

	w.Header().Set("Content-Type", "application/json")
	if response.Error != "" {
		w.WriteHeader(http.StatusInternalServerError)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func fetchTabletServers() TabletServersResponse {
	// Create client
	client := rpc.NewClient(masterAddr)

	// Connect to master
	if err := client.Connect(); err != nil {
		return TabletServersResponse{
			Error: fmt.Sprintf("Failed to connect to master: %v", err),
		}
	}
	defer client.Close()

	// Create request
	request := &pb.ListTabletServersRequestPB{
		PrimaryOnly: proto.Bool(false),
	}

	// Create response
	response := &pb.ListTabletServersResponsePB{}

	// Make RPC call
	err := client.Call("yb.master.MasterService", "ListTabletServers", request, response)
	if err != nil {
		return TabletServersResponse{
			Error: fmt.Sprintf("RPC failed: %v", err),
		}
	}

	// Check for error in response
	if response.Error != nil {
		return TabletServersResponse{
			Error: fmt.Sprintf("Master returned error: %s", response.Error.Status.GetMessage()),
		}
	}

	// Convert to JSON-friendly format
	servers := make([]TabletServer, 0, len(response.Servers))
	now := time.Now()

	for _, server := range response.Servers {
		uuid := hex.EncodeToString(server.InstanceId.GetPermanentUuid())

		ts := TabletServer{
			UUID:           uuid,
			SequenceNumber: server.InstanceId.GetInstanceSeqno(),
		}

		if server.Registration != nil && server.Registration.Common != nil {
			reg := server.Registration.Common

			// Use broadcast addresses if available, otherwise private addresses
			addresses := reg.BroadcastAddresses
			if len(addresses) == 0 {
				addresses = reg.PrivateRpcAddresses
			}

			if len(addresses) > 0 {
				ts.RPCAddress = fmt.Sprintf("%s:%d", addresses[0].GetHost(), addresses[0].GetPort())
			}

			ts.PlacementCloud = reg.GetPlacementCloud()
			ts.PlacementRegion = reg.GetPlacementRegion()
			ts.PlacementZone = reg.GetPlacementZone()
		}

		if server.Alive != nil {
			ts.Alive = server.GetAlive()
		}

		if server.MillisSinceHeartbeat != nil {
			ts.MillisSinceHeartbeat = server.GetMillisSinceHeartbeat()
			ts.LastHeartbeat = now.Add(-time.Duration(ts.MillisSinceHeartbeat) * time.Millisecond)
		}

		servers = append(servers, ts)
	}

	return TabletServersResponse{
		Servers: servers,
		Count:   len(servers),
	}
}

func handleListMasters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := fetchMasters()

	w.Header().Set("Content-Type", "application/json")
	if response.Error != "" {
		w.WriteHeader(http.StatusInternalServerError)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func fetchMasters() MastersResponse {
	client := rpc.NewClient(masterAddr)

	if err := client.Connect(); err != nil {
		return MastersResponse{
			Error: fmt.Sprintf("Failed to connect to master: %v", err),
		}
	}
	defer client.Close()

	request := &pb.ListMastersRequestPB{}
	response := &pb.ListMastersResponsePB{}

	err := client.Call("yb.master.MasterService", "ListMasters", request, response)
	if err != nil {
		return MastersResponse{
			Error: fmt.Sprintf("RPC failed: %v", err),
		}
	}

	if response.Error != nil {
		return MastersResponse{
			Error: fmt.Sprintf("Master returned error: %s", response.Error.Status.GetMessage()),
		}
	}

	masters := make([]Master, 0, len(response.Masters))

	for _, master := range response.Masters {
		// Skip entries with errors
		if master.Error != nil {
			continue
		}

		// Skip entries without instance ID
		if master.InstanceId == nil {
			continue
		}

		m := Master{
			UUID: hex.EncodeToString(master.InstanceId.GetPermanentUuid()),
		}

		if master.Registration != nil {
			reg := master.Registration

			// RPC addresses
			addresses := reg.BroadcastAddresses
			if len(addresses) == 0 {
				addresses = reg.PrivateRpcAddresses
			}
			if len(addresses) > 0 {
				m.RPCAddress = fmt.Sprintf("%s:%d", addresses[0].GetHost(), addresses[0].GetPort())
			}

			// HTTP addresses
			httpAddrs := reg.HttpAddresses
			if len(httpAddrs) > 0 {
				m.HTTPAddress = fmt.Sprintf("%s:%d", httpAddrs[0].GetHost(), httpAddrs[0].GetPort())
			}

			m.Cloud = reg.GetPlacementCloud()
			m.Region = reg.GetPlacementRegion()
			m.Zone = reg.GetPlacementZone()
		}

		// Convert role
		switch master.GetRole() {
		case pb.PeerRole_LEADER:
			m.Role = "LEADER"
		case pb.PeerRole_FOLLOWER:
			m.Role = "FOLLOWER"
		case pb.PeerRole_CANDIDATE:
			m.Role = "CANDIDATE"
		default:
			m.Role = "UNKNOWN"
		}

		masters = append(masters, m)
	}

	return MastersResponse{
		Masters: masters,
		Count:   len(masters),
	}
}

func handleClusterConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := fetchClusterConfig()

	w.Header().Set("Content-Type", "application/json")
	if response.Error != "" {
		w.WriteHeader(http.StatusInternalServerError)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func fetchClusterConfig() ClusterConfigResponse {
	client := rpc.NewClient(masterAddr)

	if err := client.Connect(); err != nil {
		return ClusterConfigResponse{
			Error: fmt.Sprintf("Failed to connect to master: %v", err),
		}
	}
	defer client.Close()

	request := &pb.GetMasterClusterConfigRequestPB{}
	response := &pb.GetMasterClusterConfigResponsePB{}

	err := client.Call("yb.master.MasterService", "GetMasterClusterConfig", request, response)
	if err != nil {
		return ClusterConfigResponse{
			Error: fmt.Sprintf("RPC failed: %v", err),
		}
	}

	if response.Error != nil {
		return ClusterConfigResponse{
			Error: fmt.Sprintf("Master returned error: %s", response.Error.Status.GetMessage()),
		}
	}

	config := ClusterConfig{}

	if response.ClusterConfig != nil {
		cc := response.ClusterConfig
		config.Version = int(cc.GetVersion())
		config.ClusterUUID = cc.GetClusterUuid()

		// Handle replication info
		if cc.ReplicationInfo != nil {
			repInfo := make(map[string]interface{})

			if cc.ReplicationInfo.LiveReplicas != nil {
				repInfo["live_replicas"] = map[string]interface{}{
					"num_replicas": cc.ReplicationInfo.LiveReplicas.GetNumReplicas(),
				}
			}

			// Convert affinitized leaders from CloudInfoPB to strings
			if len(cc.ReplicationInfo.AffinitizedLeaders) > 0 {
				leaders := make([]string, 0, len(cc.ReplicationInfo.AffinitizedLeaders))
				for _, leader := range cc.ReplicationInfo.AffinitizedLeaders {
					leaders = append(leaders, fmt.Sprintf("%s.%s.%s",
						leader.GetPlacementCloud(),
						leader.GetPlacementRegion(),
						leader.GetPlacementZone()))
				}
				repInfo["affinitized_leaders"] = leaders
			}

			config.ReplicationInfo = repInfo
		}

		// Handle encryption info
		if cc.EncryptionInfo != nil {
			config.EncryptionEnabled = cc.EncryptionInfo.GetEncryptionEnabled()
		}
	}

	return ClusterConfigResponse{
		Config: config,
	}
}

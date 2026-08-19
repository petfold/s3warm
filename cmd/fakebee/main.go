// Command fakebee is an in-memory stand-in for the subset of the Bee HTTP
// API that s3warm uses: /bytes (upload/download with Range), /health and
// /stamps. Bee removed its `dev` mode, so this fills the dev/CI slot — the
// s3-tests harness and docker-compose stack run against it anywhere, while
// conformance manifests are curated against a real node.
//
// References are sha256 hashes, NOT Swarm BMT hashes: fine for exercising
// the gateway, useless for talking to the real network.
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

func main() {
	listen := flag.String("listen", ":1633", "listen address")
	flag.Parse()

	var mu sync.RWMutex
	blobs := map[string][]byte{}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /bytes", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("swarm-postage-batch-id") == "" {
			w.WriteHeader(http.StatusPaymentRequired)
			io.WriteString(w, `{"code":402,"message":"batch not found"}`) //nolint:errcheck
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sum := sha256.Sum256(data)
		ref := hex.EncodeToString(sum[:])
		mu.Lock()
		blobs[ref] = data
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"reference": ref}) //nolint:errcheck
	})
	mux.HandleFunc("GET /bytes/{ref}", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		data, ok := blobs[r.PathValue("ref")]
		mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":404,"message":"not found"}`) //nolint:errcheck
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"status":"ok","version":"fakebee"}`) //nolint:errcheck
	})
	mux.HandleFunc("GET /stamps/{id}", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"batchID": r.PathValue("id"), "exists": true, "usable": true,
			"utilization": 0, "utilizationRatio": 0.0,
			"depth": 24, "bucketDepth": 16, "amount": "100000000000",
			"batchTTL": 86400 * 365, "immutableFlag": false,
		})
	})
	mux.HandleFunc("POST /stamps/{amount}/{depth}", func(w http.ResponseWriter, _ *http.Request) {
		var id [32]byte
		rand.Read(id[:]) //nolint:errcheck
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"batchID": hex.EncodeToString(id[:])}) //nolint:errcheck
	})

	log.Printf("fakebee listening on %s (in-memory, NOT a Swarm node)", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

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
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"sync"
	"time"
)

func main() {
	listen := flag.String("listen", ":1633", "listen address")
	flag.Parse()

	var mu sync.RWMutex
	blobs := map[string][]byte{}
	// ACT emulation: refs uploaded with swarm-act require the matching
	// history + the publisher key on download; grantee lists are stored by
	// reference and each mutation mints a new history (like Bee).
	const publisherKey = "02fa4ebeefa4ebeefa4ebeefa4ebeefa4ebeefa4ebeefa4ebeefa4ebeefa4eb"
	actRefs := map[string]string{}    // act-protected ref → history at upload
	histories := map[string]bool{}    // known histories
	grantees := map[string][]string{} // grantee-list ref → pubkeys
	newHex := func(n int) string {
		b := make([]byte, n)
		rand.Read(b[:]) //nolint:errcheck
		return hex.EncodeToString(b)
	}

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
		if r.Header.Get("swarm-encrypt") == "true" {
			// Encrypted references are 64 bytes: hash plus decryption key.
			ref += hex.EncodeToString(sum[:])
		}
		mu.Lock()
		if r.Header.Get("swarm-act") == "true" {
			history := r.Header.Get("swarm-act-history-address")
			if history == "" {
				history = newHex(32)
			} else if !histories[history] {
				mu.Unlock()
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"code":400,"message":"unknown act history"}`) //nolint:errcheck
				return
			}
			histories[history] = true
			// ACT references are encrypted references (64 bytes) distinct
			// from the plain content address.
			ref = newHex(64)
			actRefs[ref] = history
			w.Header().Set("Swarm-Act-History-Address", history)
		}
		blobs[ref] = data
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"reference": ref}) //nolint:errcheck
	})
	mux.HandleFunc("GET /bytes/{ref}", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		data, ok := blobs[r.PathValue("ref")]
		actHistory, isAct := actRefs[r.PathValue("ref")]
		mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":404,"message":"not found"}`) //nolint:errcheck
			return
		}
		if isAct {
			_ = actHistory // any known history may unlock it after patches
			if r.Header.Get("swarm-act-history-address") == "" ||
				r.Header.Get("swarm-act-publisher") != publisherKey {
				w.WriteHeader(http.StatusNotFound)
				io.WriteString(w, `{"code":404,"message":"act credentials required"}`) //nolint:errcheck
				return
			}
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	})
	mux.HandleFunc("GET /addresses", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"overlay": "fa4ebee0", "publicKey": publisherKey,
		})
	})
	mux.HandleFunc("POST /grantee", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("swarm-postage-batch-id") == "" {
			w.WriteHeader(http.StatusPaymentRequired)
			io.WriteString(w, `{"code":402,"message":"batch not found"}`) //nolint:errcheck
			return
		}
		var req struct {
			Grantees []string `json:"grantees"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Grantees) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		ref, history := newHex(64), newHex(32)
		grantees[ref] = req.Grantees
		histories[history] = true
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"ref": ref, "historyref": history}) //nolint:errcheck
	})
	mux.HandleFunc("GET /grantee/{ref}", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		list, ok := grantees[r.PathValue("ref")]
		mu.RUnlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(list) //nolint:errcheck
	})
	mux.HandleFunc("PATCH /grantee/{ref}", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("swarm-act-history-address") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var req struct {
			Add    []string `json:"add"`
			Revoke []string `json:"revoke"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		old, ok := grantees[r.PathValue("ref")]
		if !ok {
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		revoked := map[string]bool{}
		for _, k := range req.Revoke {
			revoked[k] = true
		}
		var next []string
		for _, k := range old {
			if !revoked[k] {
				next = append(next, k)
			}
		}
		for _, k := range req.Add {
			if !revoked[k] {
				next = append(next, k)
			}
		}
		ref, history := newHex(64), newHex(32)
		grantees[ref] = next
		histories[history] = true
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"ref": ref, "historyref": history}) //nolint:errcheck
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
	// Chequebook/wallet: the auto top-up keeper's surface. Starts low so a
	// dev stack exercises the deposit path.
	var moneyMu sync.Mutex
	chequebook := big.NewInt(5e15)   // 0.5 xBZZ
	wallet := big.NewInt(100 * 1e16) // 100 xBZZ
	mux.HandleFunc("GET /chequebook/balance", func(w http.ResponseWriter, _ *http.Request) {
		moneyMu.Lock()
		defer moneyMu.Unlock()
		fmt.Fprintf(w, `{"totalBalance":"%s","availableBalance":"%s"}`, chequebook, chequebook)
	})
	mux.HandleFunc("GET /wallet", func(w http.ResponseWriter, _ *http.Request) {
		moneyMu.Lock()
		defer moneyMu.Unlock()
		fmt.Fprintf(w, `{"bzzBalance":"%s","nativeTokenBalance":"1000000000000000000"}`, wallet)
	})
	mux.HandleFunc("POST /chequebook/deposit", func(w http.ResponseWriter, r *http.Request) {
		amt, ok := new(big.Int).SetString(r.URL.Query().Get("amount"), 10)
		if !ok || amt.Sign() <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		moneyMu.Lock()
		wallet.Sub(wallet, amt)
		chequebook.Add(chequebook, amt)
		moneyMu.Unlock()
		io.WriteString(w, `{"transactionHash":"0xfa4ebee"}`) //nolint:errcheck
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

package stamp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petfold/s3warm/internal/bee"
)

// fakeStampNode serves the batch/wallet/chainstate surface the autopilot
// reads and records the topup/dilute calls it makes.
type fakeStampNode struct {
	batches   map[string]map[string]any // batches the node issued
	foreign   map[string]map[string]any // resolvable but issued elsewhere
	walletBZZ *big.Int
	native    *big.Int
	price     int64

	topups  map[string]*big.Int // batch -> amount per chunk
	dilutes map[string]int      // batch -> new depth
}

func batchState(usable bool, ttlSeconds int64, utilization, depth, bucketDepth int, immutable bool) map[string]any {
	return map[string]any{
		"exists": true, "usable": usable,
		"utilization": utilization, "utilizationRatio": 0.0,
		"depth": depth, "bucketDepth": bucketDepth,
		"amount": "1000", "batchTTL": ttlSeconds, "immutableFlag": immutable,
	}
}

func (f *fakeStampNode) server(t *testing.T) *httptest.Server {
	t.Helper()
	f.topups = map[string]*big.Int{}
	f.dilutes = map[string]int{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stamps", func(w http.ResponseWriter, _ *http.Request) {
		var list []map[string]any
		for id, b := range f.batches {
			entry := map[string]any{"batchID": id}
			for k, v := range b {
				entry[k] = v
			}
			list = append(list, entry)
		}
		json.NewEncoder(w).Encode(map[string]any{"stamps": list}) //nolint:errcheck
	})
	mux.HandleFunc("GET /stamps/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		b := f.batches[id]
		if b == nil {
			b = f.foreign[id]
		}
		if b == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		entry := map[string]any{"batchID": id}
		for k, v := range b {
			entry[k] = v
		}
		json.NewEncoder(w).Encode(entry) //nolint:errcheck
	})
	mux.HandleFunc("GET /chainstate", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"currentPrice":"%d"}`, f.price)
	})
	mux.HandleFunc("GET /wallet", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"bzzBalance":"%s","nativeTokenBalance":"%s"}`, f.walletBZZ, f.native)
	})
	mux.HandleFunc("PATCH /stamps/topup/{id}/{amount}", func(w http.ResponseWriter, r *http.Request) {
		if f.batches[r.PathValue("id")] == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		amt, _ := new(big.Int).SetString(r.PathValue("amount"), 10)
		f.topups[r.PathValue("id")] = amt
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, `{"batchID":"x"}`) //nolint:errcheck
	})
	mux.HandleFunc("PATCH /stamps/dilute/{id}/{depth}", func(w http.ResponseWriter, r *http.Request) {
		if f.batches[r.PathValue("id")] == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var d int
		fmt.Sscanf(r.PathValue("depth"), "%d", &d) //nolint:errcheck
		f.dilutes[r.PathValue("id")] = d
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, `{"batchID":"x"}`) //nolint:errcheck
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// runPass primes the manager cache with the given ids (as the gateway's use
// of the batches would) and runs one autopilot pass.
func runPass(t *testing.T, f *fakeStampNode, ids ...string) {
	t.Helper()
	srv := f.server(t)
	client := bee.New(srv.URL)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(client, logger)
	ctx := context.Background()
	for _, id := range ids {
		if _, err := mgr.Get(ctx, id); err != nil {
			t.Fatalf("prime %s: %v", id, err)
		}
	}
	a := NewAutopilot(mgr, client, true, 30, 90, 0.85, logger)
	a.pass(ctx)
}

const day = int64(24 * 60 * 60)

func TestAutopilotTopup(t *testing.T) {
	f := &fakeStampNode{
		batches: map[string]map[string]any{
			// 10 days left, 90 targeted: buy ~80 days' worth.
			"low": batchState(true, 10*day, 0, 20, 16, false),
			// 60 days left: healthy, untouched.
			"ok": batchState(true, 60*day, 0, 20, 16, false),
		},
		walletBZZ: big.NewInt(1e18), native: big.NewInt(1e18), price: 24000,
	}
	runPass(t, f, "low", "ok")
	if f.topups["ok"] != nil || len(f.dilutes) != 0 {
		t.Fatalf("healthy batch touched: topups=%v dilutes=%v", f.topups, f.dilutes)
	}
	amt := f.topups["low"]
	if amt == nil {
		t.Fatal("low-TTL batch was not topped up")
	}
	// ~80 days of blocks at 5s, times price 24000.
	want := new(big.Int).Mul(big.NewInt(80*day/5+1), big.NewInt(24000))
	if amt.Cmp(want) != 0 {
		t.Fatalf("topup amount = %s, want %s", amt, want)
	}
}

func TestAutopilotDilute(t *testing.T) {
	f := &fakeStampNode{
		batches: map[string]map[string]any{
			// 15/16 buckets used = 94%, mutable, plenty of TTL.
			"full": batchState(true, 90*day, 15, 20, 16, false),
			// Same fill but immutable: warn only, never dilute.
			"fullimm": batchState(true, 90*day, 15, 20, 16, true),
		},
		walletBZZ: big.NewInt(1e18), native: big.NewInt(1e18), price: 24000,
	}
	runPass(t, f, "full", "fullimm")
	if d := f.dilutes["full"]; d != 21 {
		t.Fatalf("dilute depth = %d, want 21", d)
	}
	if _, ok := f.dilutes["fullimm"]; ok {
		t.Fatal("immutable batch was diluted")
	}
	if len(f.topups) != 0 {
		t.Fatalf("unexpected topups: %v", f.topups)
	}
}

func TestAutopilotOneActionPerCycle(t *testing.T) {
	f := &fakeStampNode{
		batches: map[string]map[string]any{
			// Both nearly full AND nearly expired: dilute wins this cycle,
			// the topup is recomputed on post-dilution state next cycle.
			"both": batchState(true, 5*day, 15, 20, 16, false),
		},
		walletBZZ: big.NewInt(1e18), native: big.NewInt(1e18), price: 24000,
	}
	runPass(t, f, "both")
	if f.dilutes["both"] != 21 {
		t.Fatal("expected a dilute")
	}
	if len(f.topups) != 0 {
		t.Fatalf("topup in the same cycle as a dilute: %v", f.topups)
	}
}

func TestAutopilotGuards(t *testing.T) {
	f := &fakeStampNode{
		batches: map[string]map[string]any{
			"low": batchState(true, 10*day, 0, 20, 16, false),
		},
		foreign: map[string]map[string]any{
			// Used by the gateway but issued elsewhere: never managed.
			"borrowed": batchState(true, 2*day, 0, 20, 16, false),
		},
		// Wallet cannot afford ~80 days across 2^20 chunks.
		walletBZZ: big.NewInt(1000), native: big.NewInt(1e18), price: 24000,
	}
	runPass(t, f, "low", "borrowed")
	if len(f.topups) != 0 || len(f.dilutes) != 0 {
		t.Fatalf("acted despite guards: topups=%v dilutes=%v", f.topups, f.dilutes)
	}

	// No gas: even an affordable topup is skipped.
	f2 := &fakeStampNode{
		batches:   map[string]map[string]any{"low": batchState(true, 10*day, 0, 20, 16, false)},
		walletBZZ: big.NewInt(1e18), native: big.NewInt(0), price: 24000,
	}
	runPass(t, f2, "low")
	if len(f2.topups) != 0 {
		t.Fatalf("topped up without gas: %v", f2.topups)
	}
}

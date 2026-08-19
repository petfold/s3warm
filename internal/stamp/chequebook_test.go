package stamp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/petfold/s3warm/internal/bee"
)

type fakeChequebook struct {
	available *big.Int
	walletBZZ *big.Int
	native    *big.Int
	deposited *big.Int // nil until a deposit happens
}

func (f *fakeChequebook) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /chequebook/balance", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"totalBalance":"%s","availableBalance":"%s"}`, f.available, f.available)
	})
	mux.HandleFunc("GET /wallet", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"bzzBalance":"%s","nativeTokenBalance":"%s"}`, f.walletBZZ, f.native)
	})
	mux.HandleFunc("POST /chequebook/deposit", func(w http.ResponseWriter, r *http.Request) {
		amt, ok := new(big.Int).SetString(r.URL.Query().Get("amount"), 10)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.deposited = amt
		io.WriteString(w, `{"transactionHash":"0xfeed"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// newTestChequebook uses min 0.2, target 1, reserve 1 — the defaults.
func newTestChequebook(t *testing.T, f *fakeChequebook) *Chequebook {
	t.Helper()
	srv := f.server(t)
	return NewChequebook(bee.New(srv.URL), 0.2, 1, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestChequebookTopUp(t *testing.T) {
	bzz := func(v float64) *big.Int { return bzzToPlur(v) }

	t.Run("below min tops up to target", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.1), walletBZZ: bzz(100), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited == nil || f.deposited.Cmp(bzz(0.9)) != 0 {
			t.Fatalf("deposited = %v, want %v", f.deposited, bzz(0.9))
		}
	})

	t.Run("above min does nothing", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.5), walletBZZ: bzz(100), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited != nil {
			t.Fatalf("unexpected deposit %v", f.deposited)
		}
	})

	t.Run("no gas skips", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.1), walletBZZ: bzz(100), native: big.NewInt(0)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited != nil {
			t.Fatalf("unexpected deposit %v", f.deposited)
		}
	})

	t.Run("postage reserve is never taken", func(t *testing.T) {
		// Wallet at the reserve: bandwidth must not raid postage funds.
		f := &fakeChequebook{available: bzz(0.1), walletBZZ: bzz(1), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited != nil {
			t.Fatalf("unexpected deposit %v", f.deposited)
		}
	})

	t.Run("deposits only the surplus above the reserve", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.1), walletBZZ: bzz(1.3), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited == nil || f.deposited.Cmp(bzz(0.3)) != 0 {
			t.Fatalf("deposited = %v, want %v", f.deposited, bzz(0.3))
		}
	})

	t.Run("disabled by min zero", func(t *testing.T) {
		if NewChequebook(nil, 0, 1, 1, nil) != nil {
			t.Fatal("min 0 must disable the keeper")
		}
	})
}

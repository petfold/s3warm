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

func newTestChequebook(t *testing.T, f *fakeChequebook) *Chequebook {
	t.Helper()
	srv := f.server(t)
	return NewChequebook(bee.New(srv.URL), 1, 5, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestChequebookTopUp(t *testing.T) {
	bzz := func(v float64) *big.Int { return bzzToPlur(v) }

	t.Run("below min tops up to target", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.2), walletBZZ: bzz(100), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited == nil || f.deposited.Cmp(bzz(4.8)) != 0 {
			t.Fatalf("deposited = %v, want %v", f.deposited, bzz(4.8))
		}
	})

	t.Run("above min does nothing", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(2), walletBZZ: bzz(100), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited != nil {
			t.Fatalf("unexpected deposit %v", f.deposited)
		}
	})

	t.Run("no gas skips", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.2), walletBZZ: bzz(100), native: big.NewInt(0)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited != nil {
			t.Fatalf("unexpected deposit %v", f.deposited)
		}
	})

	t.Run("short wallet deposits what it has", func(t *testing.T) {
		f := &fakeChequebook{available: bzz(0.2), walletBZZ: bzz(1.5), native: big.NewInt(1e18)}
		newTestChequebook(t, f).check(context.Background())
		if f.deposited == nil || f.deposited.Cmp(bzz(1.5)) != 0 {
			t.Fatalf("deposited = %v, want %v", f.deposited, bzz(1.5))
		}
	})

	t.Run("disabled by min zero", func(t *testing.T) {
		if NewChequebook(nil, 0, 5, nil) != nil {
			t.Fatal("min 0 must disable the keeper")
		}
	})
}

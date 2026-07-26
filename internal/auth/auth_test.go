package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestOIDCJWKSVerifierAcceptsRS256Token(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	jwks := buildJWKS("k1", &priv.PublicKey)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer jwksSrv.Close()

	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"sub":   "oidc-user-1",
		"iss":   "https://issuer.example",
		"aud":   "mini-k8s-api",
		"roles": []string{"operator"},
		"iat":   now.Unix(),
		"exp":   now.Add(10 * time.Minute).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "k1"
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	v := NewVerifier("oidc", "", "", "https://issuer.example", "mini-k8s-api", jwksSrv.URL)
	p, err := v.VerifyBearer("Bearer " + signed)
	if err != nil {
		t.Fatalf("verify oidc bearer: %v", err)
	}
	if p.Source != "oidc" {
		t.Fatalf("expected oidc source, got %s", p.Source)
	}
	if p.Subject != "oidc-user-1" {
		t.Fatalf("expected subject oidc-user-1, got %s", p.Subject)
	}
	if len(p.Roles) != 1 || p.Roles[0] != "operator" {
		t.Fatalf("unexpected roles: %+v", p.Roles)
	}
}

func TestHybridFallsBackToOIDC(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	jwks := buildJWKS("k2", &priv.PublicKey)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer jwksSrv.Close()

	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"sub":   "hybrid-user",
		"iss":   "https://issuer.example",
		"aud":   []string{"mini-k8s-api"},
		"roles": []string{"viewer"},
		"iat":   now.Unix(),
		"exp":   now.Add(10 * time.Minute).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "k2"
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	v := NewVerifier("hybrid", "dev-token", "", "https://issuer.example", "mini-k8s-api", jwksSrv.URL)
	p, err := v.VerifyBearer("Bearer " + signed)
	if err != nil {
		t.Fatalf("verify hybrid bearer: %v", err)
	}
	if p.Source != "oidc" {
		t.Fatalf("expected hybrid fallback source oidc, got %s", p.Source)
	}
}

func buildJWKS(kid string, pub *rsa.PublicKey) map[string]any {
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes())
	return map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA",
			"kid": kid,
			"alg": "RS256",
			"use": "sig",
			"n":   n,
			"e":   e,
		}},
	}
}

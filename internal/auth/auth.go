package auth

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Mode string

const (
	ModeStatic Mode = "static"
	ModeJWT    Mode = "jwt"
	ModeHybrid Mode = "hybrid"
	ModeOIDC   Mode = "oidc"
)

type Principal struct {
	Subject string
	Roles   []string
	Source  string
}

type Verifier struct {
	mode      Mode
	staticTok string
	jwtSecret string
	issuer    string
	audience  string
	jwksURL   string

	jwksMu           sync.RWMutex
	resolvedJWKKeys  map[string]any
	lastJWKSRefresh  time.Time
	jwksRefreshAfter time.Duration
}

func NewVerifier(mode, staticTok, jwtSecret, issuer, audience, jwksURL string) *Verifier {
	m := Mode(strings.ToLower(strings.TrimSpace(mode)))
	if m == "" {
		m = ModeStatic
	}
	return &Verifier{
		mode:      m,
		staticTok: strings.TrimSpace(staticTok),
		jwtSecret: strings.TrimSpace(jwtSecret),
		issuer:    strings.TrimSpace(issuer),
		audience:  strings.TrimSpace(audience),
		jwksURL:   strings.TrimSpace(jwksURL),

		resolvedJWKKeys:  map[string]any{},
		jwksRefreshAfter: 5 * time.Minute,
	}
}

func (v *Verifier) VerifyBearer(authz string) (Principal, error) {
	authz = strings.TrimSpace(authz)
	if !strings.HasPrefix(authz, "Bearer ") {
		return Principal{}, errors.New("missing bearer token")
	}
	token := strings.TrimSpace(strings.TrimPrefix(authz, "Bearer "))
	if token == "" {
		return Principal{}, errors.New("empty bearer token")
	}

	switch v.mode {
	case ModeStatic:
		return v.verifyStatic(token)
	case ModeJWT:
		return v.verifyJWT(token)
	case ModeOIDC:
		return v.verifyOIDC(token)
	case ModeHybrid:
		if p, err := v.verifyStatic(token); err == nil {
			return p, nil
		}
		if p, err := v.verifyJWT(token); err == nil {
			return p, nil
		}
		return v.verifyOIDC(token)
	default:
		return Principal{}, fmt.Errorf("unsupported auth mode: %s", v.mode)
	}
}

func (v *Verifier) verifyStatic(token string) (Principal, error) {
	if v.staticTok == "" || token != v.staticTok {
		return Principal{}, errors.New("invalid static token")
	}
	return Principal{Subject: "static-token", Roles: []string{"admin"}, Source: "static"}, nil
}

func (v *Verifier) verifyJWT(token string) (Principal, error) {
	if v.jwtSecret == "" {
		return Principal{}, errors.New("jwt secret not configured")
	}
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %s", t.Method.Alg())
		}
		return []byte(v.jwtSecret), nil
	})
	if err != nil || !parsed.Valid {
		return Principal{}, errors.New("invalid jwt token")
	}

	if v.issuer != "" {
		if got, _ := claims["iss"].(string); got != v.issuer {
			return Principal{}, errors.New("invalid issuer")
		}
	}
	if v.audience != "" {
		if got, _ := claims["aud"].(string); got != v.audience {
			return Principal{}, errors.New("invalid audience")
		}
	}
	sub, _ := claims["sub"].(string)
	roles := extractRoles(claims)
	if len(roles) == 0 {
		roles = []string{"viewer"}
	}
	return Principal{Subject: sub, Roles: roles, Source: "jwt"}, nil
}

func (v *Verifier) verifyOIDC(token string) (Principal, error) {
	if v.jwksURL == "" {
		return Principal{}, errors.New("jwks url not configured")
	}
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != jwt.SigningMethodRS256.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %s", t.Method.Alg())
		}
		kid, _ := t.Header["kid"].(string)
		if strings.TrimSpace(kid) == "" {
			return nil, errors.New("missing kid in jwt header")
		}
		return v.resolveJWKKeyByKID(kid)
	})
	if err != nil || !parsed.Valid {
		return Principal{}, errors.New("invalid oidc jwt token")
	}

	if v.issuer != "" {
		if got, _ := claims["iss"].(string); got != v.issuer {
			return Principal{}, errors.New("invalid issuer")
		}
	}
	if v.audience != "" {
		if !claimHasAudience(claims["aud"], v.audience) {
			return Principal{}, errors.New("invalid audience")
		}
	}
	sub, _ := claims["sub"].(string)
	roles := extractRoles(claims)
	if len(roles) == 0 {
		roles = []string{"viewer"}
	}
	return Principal{Subject: sub, Roles: roles, Source: "oidc"}, nil
}

func (v *Verifier) resolveJWKKeyByKID(kid string) (any, error) {
	now := time.Now().UTC()

	v.jwksMu.RLock()
	if key, ok := v.resolvedJWKKeys[kid]; ok && now.Sub(v.lastJWKSRefresh) < v.jwksRefreshAfter {
		v.jwksMu.RUnlock()
		return key, nil
	}
	v.jwksMu.RUnlock()

	v.jwksMu.Lock()
	defer v.jwksMu.Unlock()
	if key, ok := v.resolvedJWKKeys[kid]; ok && now.Sub(v.lastJWKSRefresh) < v.jwksRefreshAfter {
		return key, nil
	}
	if err := v.refreshJWKSLocked(); err != nil {
		return nil, err
	}
	key, ok := v.resolvedJWKKeys[kid]
	if !ok {
		return nil, fmt.Errorf("kid not found in jwks: %s", kid)
	}
	return key, nil
}

func (v *Verifier) refreshJWKSLocked() error {
	resp, err := http.Get(v.jwksURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("jwks endpoint returned status %d", resp.StatusCode)
	}
	var payload struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	keys := map[string]any{}
	for _, k := range payload.Keys {
		if strings.ToUpper(strings.TrimSpace(k.Kty)) != "RSA" || strings.TrimSpace(k.Kid) == "" {
			continue
		}
		pub, err := jwkRSAToPublicKey(k.N, k.E)
		if err != nil {
			continue
		}
		keys[k.Kid] = pub
	}
	v.resolvedJWKKeys = keys
	v.lastJWKSRefresh = time.Now().UTC()
	if len(v.resolvedJWKKeys) == 0 {
		return errors.New("no usable rsa keys found in jwks")
	}
	return nil
}

func jwkRSAToPublicKey(nRaw, eRaw string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(nRaw))
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(eRaw))
	if err != nil {
		return nil, err
	}
	n := new(big.Int).SetBytes(nBytes)
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}
	if e <= 0 {
		return nil, errors.New("invalid rsa exponent")
	}
	return &rsa.PublicKey{N: n, E: e}, nil
}

func claimHasAudience(raw any, expected string) bool {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return true
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v) == expected
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) == expected {
				return true
			}
		}
	}
	return false
}

func extractRoles(claims jwt.MapClaims) []string {
	roles := []string{}
	if raw, ok := claims["roles"]; ok {
		switch v := raw.(type) {
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
					roles = append(roles, strings.TrimSpace(s))
				}
			}
		case []string:
			for _, s := range v {
				if strings.TrimSpace(s) != "" {
					roles = append(roles, strings.TrimSpace(s))
				}
			}
		case string:
			if strings.TrimSpace(v) != "" {
				roles = append(roles, strings.TrimSpace(v))
			}
		}
	}
	return roles
}

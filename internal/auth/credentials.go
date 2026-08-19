package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// credentialsFileEntry is one record of the -credentials JSON file
// (design §8 layer 2): an S3 access key pair bound to a tenant.
type credentialsFileEntry struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	// Tenant labels the owner every bucket this key creates belongs to.
	// Empty (or "root") makes the key a root key: unrestricted.
	Tenant string `json:"tenant"`
}

// LoadCredentialsFile reads a JSON credentials file — an array of
// {accessKey, secretKey, tenant} objects — into the given credential set.
// Entries with tenant "" or "root" are root keys.
func LoadCredentialsFile(path string, into StaticCredentials) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var entries []credentialsFileEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("credentials file %s: %w", path, err)
	}
	for i, e := range entries {
		if e.AccessKey == "" || e.SecretKey == "" {
			return fmt.Errorf("credentials file %s: entry %d is missing accessKey or secretKey", path, i)
		}
		if _, dup := into[e.AccessKey]; dup {
			return fmt.Errorf("credentials file %s: duplicate access key %q", path, e.AccessKey)
		}
		tenant := e.Tenant
		if strings.EqualFold(tenant, "root") {
			tenant = ""
		}
		into[e.AccessKey] = Credential{Secret: e.SecretKey, Tenant: tenant}
	}
	return nil
}

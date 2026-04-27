package tenantpool

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
)

// KVPasswordResolver fetches per-tenant role passwords from an Azure
// Key Vault. The secret name is built from a template containing the
// {tenant} placeholder; for example
//
//	"platform-cnpg-{tenant}-tenant-palbase-auth-password"
//
// resolved against tenant "acme" hits
//
//	platform-cnpg-acme-tenant-palbase-auth-password
//
// inside the configured vault.
//
// Wrap with CachingPasswordResolver for production — a raw KV call per
// pgxpool.New burns a network hop and a token-renewal lookup.
type KVPasswordResolver struct {
	cli      *azsecrets.Client
	template string
}

// NewKVPasswordResolver constructs a resolver bound to vaultName.
// Authentication uses azidentity.NewDefaultAzureCredential, which means
// callers running in AKS with Workload Identity get federated tokens
// automatically; local dev picks up `az login` credentials. The template
// must contain {tenant} as the substitution marker; missing it errors
// on construction so misconfiguration fails loud.
func NewKVPasswordResolver(vaultName, template string) (*KVPasswordResolver, error) {
	if vaultName == "" {
		return nil, errors.New("tenantpool: vaultName must not be empty")
	}
	if !strings.Contains(template, "{tenant}") {
		return nil, errors.New("tenantpool: KV secret template must include {tenant}")
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("tenantpool: azure credential: %w", err)
	}
	cli, err := azsecrets.NewClient(
		fmt.Sprintf("https://%s.vault.azure.net", vaultName), cred, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("tenantpool: azsecrets client: %w", err)
	}
	return &KVPasswordResolver{cli: cli, template: template}, nil
}

// NewKVPasswordResolverWithCredential is the credential-injecting
// constructor variant, useful in tests or when callers already have a
// chained credential they want to share.
func NewKVPasswordResolverWithCredential(vaultName, template string, cred azcore.TokenCredential) (*KVPasswordResolver, error) {
	if vaultName == "" {
		return nil, errors.New("tenantpool: vaultName must not be empty")
	}
	if !strings.Contains(template, "{tenant}") {
		return nil, errors.New("tenantpool: KV secret template must include {tenant}")
	}
	if cred == nil {
		return nil, errors.New("tenantpool: credential must not be nil")
	}
	cli, err := azsecrets.NewClient(
		fmt.Sprintf("https://%s.vault.azure.net", vaultName), cred, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("tenantpool: azsecrets client: %w", err)
	}
	return &KVPasswordResolver{cli: cli, template: template}, nil
}

// Resolve fetches the latest version of the tenant's secret. Returns
// an error if the secret doesn't exist or the value is empty —
// callers treat this as "tenant not provisioned" and surface it as a
// 5xx through the registry's ErrTenantNotFound classification.
func (r *KVPasswordResolver) Resolve(ctx context.Context, tenantID string) (string, error) {
	if tenantID == "" {
		return "", errors.New("tenant id is empty")
	}
	secretName := strings.ReplaceAll(r.template, "{tenant}", tenantID)
	resp, err := r.cli.GetSecret(ctx, secretName, "", nil)
	if err != nil {
		return "", fmt.Errorf("kv get %s: %w", secretName, err)
	}
	if resp.Value == nil || *resp.Value == "" {
		return "", fmt.Errorf("kv secret %s is empty", secretName)
	}
	return *resp.Value, nil
}

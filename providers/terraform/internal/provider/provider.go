package provider

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"time"

	"github.com/araditc/Kafdeck/providers/terraform/internal/kafdeck"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type kafdeckProvider struct{ version string }
type providerModel struct {
	Endpoint types.String `tfsdk:"endpoint"`
	Token    types.String `tfsdk:"token"`
	CAFile   types.String `tfsdk:"ca_file"`
}

func New(version string) func() provider.Provider {
	return func() provider.Provider { return &kafdeckProvider{version} }
}
func (p *kafdeckProvider) Metadata(_ context.Context, _ provider.MetadataRequest, r *provider.MetadataResponse) {
	r.TypeName = "kafdeck"
	r.Version = p.version
}
func (p *kafdeckProvider) Schema(_ context.Context, _ provider.SchemaRequest, r *provider.SchemaResponse) {
	r.Schema = schema.Schema{Description: "Typed governed Kafdeck v0.8 API provider. No direct broker, provider, database, approval or generic HTTP access.", Attributes: map[string]schema.Attribute{
		"endpoint": schema.StringAttribute{Optional: true, Description: "HTTPS Kafdeck origin, or numeric loopback HTTP. Defaults to KAFDECK_ENDPOINT."},
		"token":    schema.StringAttribute{Optional: true, Sensitive: true, Description: "Bearer access token; prefer KAFDECK_TOKEN so credentials never enter Terraform configuration/state."},
		"ca_file":  schema.StringAttribute{Optional: true, Description: "Additional trusted PEM CA file. Defaults to KAFDECK_CA_FILE. TLS verification cannot be disabled."},
	}}
}
func (p *kafdeckProvider) Configure(ctx context.Context, q provider.ConfigureRequest, r *provider.ConfigureResponse) {
	var m providerModel
	r.Diagnostics.Append(q.Config.Get(ctx, &m)...)
	if r.Diagnostics.HasError() {
		return
	}
	if m.Endpoint.IsUnknown() || m.Token.IsUnknown() || m.CAFile.IsUnknown() {
		r.Diagnostics.AddError("Unknown provider configuration", "Endpoint, credential and CA must be known before calling Kafdeck.")
		return
	}
	endpoint := m.Endpoint.ValueString()
	if endpoint == "" {
		endpoint = os.Getenv("KAFDECK_ENDPOINT")
	}
	token := m.Token.ValueString()
	if token == "" {
		token = os.Getenv("KAFDECK_TOKEN")
	}
	ca := m.CAFile.ValueString()
	if ca == "" {
		ca = os.Getenv("KAFDECK_CA_FILE")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if ca != "" {
		pem, e := os.ReadFile(ca)
		if e != nil {
			r.Diagnostics.AddError("CA configuration rejected", "Could not read trusted CA file.")
			return
		}
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			r.Diagnostics.AddError("CA configuration rejected", "CA file contains no trusted PEM certificate.")
			return
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	c, e := kafdeck.NewClient(endpoint, token, &http.Client{Transport: transport, Timeout: 15 * time.Second})
	if e != nil {
		r.Diagnostics.AddError("Provider configuration rejected", e.Error())
		return
	}
	r.ResourceData = c
	r.DataSourceData = c
}
func (p *kafdeckProvider) Resources(context.Context) []func() resource.Resource {
	return []func() resource.Resource{func() resource.Resource { return &subscriptionResource{} }}
}
func (p *kafdeckProvider) DataSources(context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{func() datasource.DataSource { return &subscriptionDataSource{} }}
}

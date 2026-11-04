package provider

import (
	"context"
	"github.com/araditc/Kafdeck/providers/terraform/internal/kafdeck"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type subscriptionDataSource struct{ client *kafdeck.Client }

func (d *subscriptionDataSource) Metadata(_ context.Context, q datasource.MetadataRequest, p *datasource.MetadataResponse) {
	p.TypeName = q.ProviderTypeName + "_notification_subscription"
}
func (d *subscriptionDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, p *datasource.SchemaResponse) {
	a := map[string]schema.Attribute{}
	for _, key := range []string{"deployment_id", "owner_id", "source_ref", "source_revision", "resource_id", "destination_ref", "subscription_state"} {
		a[key] = schema.StringAttribute{Required: true}
	}
	for _, key := range []string{"apply_plan_id", "apply_plan_fingerprint"} {
		a[key] = schema.StringAttribute{Optional: true}
	}
	for _, key := range []string{"id", "operation_id", "ownership_generation", "source_generation", "deployment_generation", "drift"} {
		a[key] = schema.StringAttribute{Computed: true}
	}
	a["revision"] = schema.Int64Attribute{Computed: true}
	a["ownership_revision"] = schema.Int64Attribute{Computed: true}
	a["event_classes"] = schema.SetAttribute{Required: true, ElementType: types.StringType}
	a["event_types"] = schema.SetAttribute{Required: true, ElementType: types.StringType}
	p.Schema = schema.Schema{Description: "Exact authorized managed subscription observation. Unknown, inaccessible, missing, conflicted or unsupported reads fail closed. Typed configuration binds the source identity; no arbitrary API paths.", Attributes: a}
}
func (d *subscriptionDataSource) Configure(_ context.Context, q datasource.ConfigureRequest, p *datasource.ConfigureResponse) {
	if q.ProviderData == nil {
		return
	}
	var ok bool
	d.client, ok = q.ProviderData.(*kafdeck.Client)
	if !ok {
		p.Diagnostics.AddError("Provider contract mismatch", "Typed Kafdeck API client required.")
	}
}
func (d *subscriptionDataSource) Read(ctx context.Context, q datasource.ReadRequest, p *datasource.ReadResponse) {
	var m subscriptionModel
	p.Diagnostics.Append(q.Config.Get(ctx, &m)...)
	if p.Diagnostics.HasError() {
		return
	}
	v, ds := m.desired(ctx)
	p.Diagnostics.Append(ds...)
	if p.Diagnostics.HasError() {
		return
	}
	o, e := d.client.Observe(ctx, v, false)
	if e != nil {
		p.Diagnostics.AddError("Authoritative observation unavailable", e.Error())
		return
	}
	if o.Drift != "InSync" {
		p.Diagnostics.AddError("OutOfSync: exact source observation differs", "Retain the sealed desired configuration. A read does not rewrite configured source content, create an approval or dispatch an effect.")
		return
	}
	p.Diagnostics.Append(m.observed(ctx, v, o)...)
	if !p.Diagnostics.HasError() {
		p.Diagnostics.Append(p.State.Set(ctx, &m)...)
	}
}

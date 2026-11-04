package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/araditc/Kafdeck/providers/terraform/internal/kafdeck"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type subscriptionModel struct {
	ID                   types.String `tfsdk:"id"`
	DeploymentID         types.String `tfsdk:"deployment_id"`
	OwnerID              types.String `tfsdk:"owner_id"`
	SourceRef            types.String `tfsdk:"source_ref"`
	SourceRevision       types.String `tfsdk:"source_revision"`
	ResourceID           types.String `tfsdk:"resource_id"`
	DestinationRef       types.String `tfsdk:"destination_ref"`
	SubscriptionState    types.String `tfsdk:"subscription_state"`
	EventClasses         types.Set    `tfsdk:"event_classes"`
	EventTypes           types.Set    `tfsdk:"event_types"`
	ApplyPlanID          types.String `tfsdk:"apply_plan_id"`
	ApplyPlanFingerprint types.String `tfsdk:"apply_plan_fingerprint"`
	OperationID          types.String `tfsdk:"operation_id"`
	Revision             types.Int64  `tfsdk:"revision"`
	OwnershipRevision    types.Int64  `tfsdk:"ownership_revision"`
	OwnershipGeneration  types.String `tfsdk:"ownership_generation"`
	SourceGeneration     types.String `tfsdk:"source_generation"`
	DeploymentGeneration types.String `tfsdk:"deployment_generation"`
	Drift                types.String `tfsdk:"drift"`
}

func (m subscriptionModel) desired(ctx context.Context) (kafdeck.Desired, diag.Diagnostics) {
	d := kafdeck.Desired{DeploymentID: m.DeploymentID.ValueString(), OwnerID: m.OwnerID.ValueString(), SourceRef: m.SourceRef.ValueString(), SourceRevision: m.SourceRevision.ValueString(), ResourceID: m.ResourceID.ValueString(), DestinationRef: m.DestinationRef.ValueString(), SubscriptionState: m.SubscriptionState.ValueString(), ApplyPlanID: m.ApplyPlanID.ValueString(), ApplyPlanFingerprint: m.ApplyPlanFingerprint.ValueString()}
	var diags diag.Diagnostics
	diags.Append(m.EventClasses.ElementsAs(ctx, &d.EventClasses, false)...)
	diags.Append(m.EventTypes.ElementsAs(ctx, &d.EventTypes, false)...)
	if !diags.HasError() {
		if e := d.Validate(); e != nil {
			diags.AddError("Invalid typed subscription", e.Error())
		}
	}
	return d, diags
}
func (m *subscriptionModel) observed(ctx context.Context, d kafdeck.Desired, o kafdeck.Observed) diag.Diagnostics {
	m.ID = types.StringValue(d.DeploymentID + "/NotificationSubscription/" + d.ResourceID)
	m.OperationID = types.StringValue(d.ApplyPlanID)
	m.Revision = types.Int64Value(*o.SubscriptionRevision)
	m.OwnershipRevision = types.Int64Value(o.OwnershipRevision)
	m.OwnershipGeneration = types.StringValue(o.OwnershipGeneration)
	m.SourceGeneration = types.StringValue(o.SourceGeneration)
	m.DeploymentGeneration = types.StringValue(o.DeploymentGeneration)
	m.Drift = types.StringValue(o.Drift)
	m.DestinationRef = types.StringValue(o.Current.DestinationRef)
	m.SubscriptionState = types.StringValue(o.Current.State)
	var diags diag.Diagnostics
	var next diag.Diagnostics
	m.EventClasses, next = types.SetValueFrom(ctx, types.StringType, o.Current.EventClasses)
	diags.Append(next...)
	m.EventTypes, next = types.SetValueFrom(ctx, types.StringType, o.Current.EventTypes)
	diags.Append(next...)
	return diags
}
func attributes() map[string]schema.Attribute {
	replace := []planmodifier.String{stringplanmodifier.RequiresReplace()}
	a := map[string]schema.Attribute{}
	for _, key := range []string{"deployment_id", "owner_id", "source_ref", "resource_id"} {
		a[key] = schema.StringAttribute{Required: true, PlanModifiers: replace}
	}
	for _, key := range []string{"source_revision", "destination_ref", "subscription_state", "apply_plan_id", "apply_plan_fingerprint"} {
		a[key] = schema.StringAttribute{Required: true}
	}
	a["event_classes"] = schema.SetAttribute{Required: true, ElementType: types.StringType}
	a["event_types"] = schema.SetAttribute{Required: true, ElementType: types.StringType}
	for _, key := range []string{"id", "operation_id", "ownership_generation", "source_generation", "deployment_generation", "drift"} {
		a[key] = schema.StringAttribute{Computed: true}
	}
	a["revision"] = schema.Int64Attribute{Computed: true}
	a["ownership_revision"] = schema.Int64Attribute{Computed: true}
	return a
}

type subscriptionResource struct{ client *kafdeck.Client }

func (r *subscriptionResource) Metadata(_ context.Context, q resource.MetadataRequest, p *resource.MetadataResponse) {
	p.TypeName = q.ProviderTypeName + "_notification_subscription"
}
func (r *subscriptionResource) Schema(_ context.Context, _ resource.SchemaRequest, p *resource.SchemaResponse) {
	p.Schema = schema.Schema{Version: 1, Description: "Managed NotificationSubscription bound to an explicit immutable backend plan. Human confirmation and independent CRITICAL approval occur outside Terraform; AwaitingApproval and ExecutionUnknown are errors, never successful applies. Delete/relinquish/retire are unadmitted.", Attributes: attributes()}
}
func (r *subscriptionResource) Configure(_ context.Context, q resource.ConfigureRequest, p *resource.ConfigureResponse) {
	if q.ProviderData == nil {
		return
	}
	var ok bool
	r.client, ok = q.ProviderData.(*kafdeck.Client)
	if !ok {
		p.Diagnostics.AddError("Provider contract mismatch", "Typed Kafdeck API client required.")
	}
}
func (r *subscriptionResource) ModifyPlan(_ context.Context, q resource.ModifyPlanRequest, p *resource.ModifyPlanResponse) {
	if q.Plan.Raw.IsNull() {
		p.Diagnostics.AddError("Destroy is not admitted", "NotificationSubscription delete, retire and relinquish are not supported by the backend. Keep Terraform state; no effect was dispatched.")
	}
}
func (r *subscriptionResource) Create(ctx context.Context, q resource.CreateRequest, p *resource.CreateResponse) {
	var m subscriptionModel
	p.Diagnostics.Append(q.Plan.Get(ctx, &m)...)
	if p.Diagnostics.HasError() {
		return
	}
	r.apply(ctx, &m, &p.Diagnostics)
	if !p.Diagnostics.HasError() {
		p.Diagnostics.Append(p.State.Set(ctx, &m)...)
	}
}
func (r *subscriptionResource) Update(ctx context.Context, q resource.UpdateRequest, p *resource.UpdateResponse) {
	var m subscriptionModel
	p.Diagnostics.Append(q.Plan.Get(ctx, &m)...)
	if p.Diagnostics.HasError() {
		return
	}
	r.apply(ctx, &m, &p.Diagnostics)
	if !p.Diagnostics.HasError() {
		p.Diagnostics.Append(p.State.Set(ctx, &m)...)
	}
}
func (r *subscriptionResource) apply(ctx context.Context, m *subscriptionModel, diags *diag.Diagnostics) {
	d, ds := m.desired(ctx)
	diags.Append(ds...)
	if diags.HasError() {
		return
	}
	o, e := r.client.Apply(ctx, d)
	if e != nil {
		diags.AddError("Governed apply did not complete", e.Error()+" The operation reference remains in apply_plan_id; do not change it to retry an unknown effect.")
		return
	}
	if o.Drift != "InSync" {
		diags.AddError("Verified readback differs", "Keep the durable operation reference and inspect backend state. No automatic retry or rollback.")
		return
	}
	diags.Append(m.observed(ctx, d, o)...)
}
func (r *subscriptionResource) Read(ctx context.Context, q resource.ReadRequest, p *resource.ReadResponse) {
	var m subscriptionModel
	p.Diagnostics.Append(q.State.Get(ctx, &m)...)
	if p.Diagnostics.HasError() {
		return
	}
	d, ds := m.desired(ctx)
	p.Diagnostics.Append(ds...)
	if p.Diagnostics.HasError() {
		return
	}
	o, e := r.client.Observe(ctx, d, false)
	if e != nil {
		p.Diagnostics.AddError("Authoritative refresh unavailable", e.Error())
		return
	}
	if o.Drift != "InSync" {
		p.Diagnostics.AddError("OutOfSync: explicit governed plan required", "Retain the sealed desired configuration and prior state. Obtain a fresh independently approved backend plan; refresh does not rewrite desired source content or dispatch effects.")
		return
	}
	if (!m.OwnershipGeneration.IsNull() && m.OwnershipGeneration.ValueString() != o.OwnershipGeneration) || (!m.SourceGeneration.IsNull() && m.SourceGeneration.ValueString() != o.SourceGeneration) || (!m.DeploymentGeneration.IsNull() && m.DeploymentGeneration.ValueString() != o.DeploymentGeneration) {
		p.Diagnostics.AddError("Ownership generation changed", "Retain prior state; explicit operator reconciliation is required.")
		return
	}
	p.Diagnostics.Append(m.observed(ctx, d, o)...)
	if !p.Diagnostics.HasError() {
		p.Diagnostics.Append(p.State.Set(ctx, &m)...)
	}
}
func (r *subscriptionResource) Delete(_ context.Context, _ resource.DeleteRequest, p *resource.DeleteResponse) {
	p.Diagnostics.AddError("Destroy is not admitted", "No backend delete/retire/relinquish was dispatched. Terraform state must be retained.")
}

// Version 0 was the unpublished integration-candidate shape. It contains all
// authority bindings already; migration preserves them without inventing grants,
// resolving an unknown operation, or making an effect request. Future versions
// need their own explicit upgrader; framework rejects unregistered versions.
func (r *subscriptionResource) UpgradeState(context.Context) map[int64]resource.StateUpgrader {
	return map[int64]resource.StateUpgrader{0: {PriorSchema: &schema.Schema{Attributes: attributes()}, StateUpgrader: func(ctx context.Context, q resource.UpgradeStateRequest, p *resource.UpgradeStateResponse) {
		var m subscriptionModel
		p.Diagnostics.Append(q.State.Get(ctx, &m)...)
		if p.Diagnostics.HasError() {
			return
		}
		_, ds := m.desired(ctx)
		p.Diagnostics.Append(ds...)
		if m.OwnershipGeneration.IsNull() || m.SourceGeneration.IsNull() || m.DeploymentGeneration.IsNull() {
			p.Diagnostics.AddError("Legacy state lacks authority bindings", "Cannot synthesize ownership, source or deployment generations; explicit authorized import required.")
			return
		}
		if !p.Diagnostics.HasError() {
			p.Diagnostics.Append(p.State.Set(ctx, &m)...)
		}
	}}}
}
func (r *subscriptionResource) ImportState(ctx context.Context, q resource.ImportStateRequest, p *resource.ImportStateResponse) {
	d, e := parseImport(q.ID)
	if e != nil {
		p.Diagnostics.AddError("Invalid exact import identity", e.Error())
		return
	}
	plan, e := r.client.ReadPlan(ctx, d)
	if e != nil || plan.State != "AppliedVerified" {
		p.Diagnostics.AddError("Import operation is not verified", "The exact supplied operation must be readable by this principal, bind this configuration, and be AppliedVerified. Import does not adopt, transfer, approve or apply.")
		return
	}
	o, e := r.client.Observe(ctx, d, true)
	if e != nil {
		p.Diagnostics.AddError("Import denied", e.Error())
		return
	}
	if plan.DeploymentGeneration != o.DeploymentGeneration {
		p.Diagnostics.AddError("Import plan generation is stale", "The completed plan and current deployment generation must match. No historical receipt grants restored authority.")
		return
	}
	if o.Drift != "InSync" {
		p.Diagnostics.AddError("Import differs from current state", "Import requires exact canonical current desired state and managed ownership; no adoption or transfer is performed.")
		return
	}
	m := model(ctx, d)
	p.Diagnostics.Append(m.observed(ctx, d, o)...)
	if !p.Diagnostics.HasError() {
		p.Diagnostics.Append(p.State.Set(ctx, &m)...)
	}
}
func parseImport(s string) (kafdeck.Desired, error) {
	var d kafdeck.Desired
	if len(s) > 16384 || kafdeck.ValidateJSON([]byte(s)) != nil {
		return d, errors.New("bounded typed JSON import required")
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.DisallowUnknownFields()
	if dec.Decode(&d) != nil {
		return d, errors.New("exact typed JSON import required")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return d, errors.New("trailing import content")
	}
	if e := d.Validate(); e != nil {
		return d, e
	}
	if d.ApplyPlanID == "" || d.ApplyPlanFingerprint == "" {
		return d, errors.New("durable operation reference required")
	}
	return d, nil
}
func model(ctx context.Context, d kafdeck.Desired) subscriptionModel {
	m := subscriptionModel{DeploymentID: types.StringValue(d.DeploymentID), OwnerID: types.StringValue(d.OwnerID), SourceRef: types.StringValue(d.SourceRef), SourceRevision: types.StringValue(d.SourceRevision), ResourceID: types.StringValue(d.ResourceID), DestinationRef: types.StringValue(d.DestinationRef), SubscriptionState: types.StringValue(d.SubscriptionState), ApplyPlanID: types.StringValue(d.ApplyPlanID), ApplyPlanFingerprint: types.StringValue(d.ApplyPlanFingerprint)}
	m.EventClasses, _ = types.SetValueFrom(ctx, types.StringType, d.EventClasses)
	m.EventTypes, _ = types.SetValueFrom(ctx, types.StringType, d.EventTypes)
	return m
}

package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/araditc/Kafdeck/providers/terraform/internal/kafdeck"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type testTransport func(*http.Request) (*http.Response, error)

func (f testTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestTwoDriftRefreshesRetainExactSealedState(t *testing.T) {
	ctx := context.Background()
	d := kafdeck.Desired{DeploymentID: "prod", OwnerID: "owner", SourceRef: "git", SourceRevision: strings.Repeat("a", 64), ResourceID: "sub", DestinationRef: "dest", SubscriptionState: "Active", EventClasses: []string{"Operational"}, EventTypes: []string{}, ApplyPlanID: "11111111-1111-1111-1111-111111111111", ApplyPlanFingerprint: strings.Repeat("b", 64)}
	m := model(ctx, d)
	gen := "22222222-2222-2222-2222-222222222222"
	m.ID = types.StringValue("prod/NotificationSubscription/sub")
	m.OperationID = types.StringValue(d.ApplyPlanID)
	m.Revision = types.Int64Value(1)
	m.OwnershipRevision = types.Int64Value(1)
	m.OwnershipGeneration = types.StringValue(gen)
	m.SourceGeneration = types.StringValue(gen)
	m.DeploymentGeneration = types.StringValue(gen)
	m.Drift = types.StringValue("InSync")
	state := tfsdk.State{Schema: schema.Schema{Version: 1, Attributes: attributes()}}
	if ds := state.Set(ctx, &m); ds.HasError() {
		t.Fatal(ds)
	}
	calls := 0
	c, e := kafdeck.NewClient("https://example.test", "secret", &http.Client{Transport: testTransport(func(q *http.Request) (*http.Response, error) {
		calls++
		if q.URL.Path != "/api/v1/declarative/observe" {
			t.Fatal("drift dispatched effect")
		}
		var req kafdeck.ManifestRequest
		if json.NewDecoder(q.Body).Decode(&req) != nil || req.Manifest != d.Manifest() {
			t.Fatal("sealed desired manifest changed after drift")
		}
		o := kafdeck.Observed{ScopeKind: "Deployment", ScopeID: "prod", Kind: "NotificationSubscription", SchemaVersion: 1, Drift: "OutOfSync", Capability: "Supported", Authorization: "Allowed", DesiredFingerprint: d.SourceRevision, ObservedAtUTC: time.Now(), Ownership: "Managed", OwnershipRevision: 1, SubscriptionRevision: new(int64(2)), SourceInSync: true, OwnershipGeneration: gen, SourceGeneration: gen, DeploymentGeneration: gen, DeploymentID: d.DeploymentID, ResourceID: d.ResourceID, OwnerRef: d.OwnerID, SourceRef: d.SourceRef, Current: &kafdeck.Subscription{DestinationRef: "other", State: "Paused", EventClasses: d.EventClasses, EventTypes: d.EventTypes}}
		b, _ := json.Marshal(o)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header)}, nil
	})})
	if e != nil {
		t.Fatal(e)
	}
	r := subscriptionResource{client: c}
	for range 2 {
		out := resource.ReadResponse{State: state}
		r.Read(ctx, resource.ReadRequest{State: state}, &out)
		if !out.Diagnostics.HasError() || !out.State.Raw.Equal(state.Raw) {
			t.Fatal("drift refresh erased/rewrote prior state")
		}
		state = out.State
	}
	if calls != 2 {
		t.Fatal(calls)
	}
	ds := subscriptionDataSource{client: c}
	var schemaOut datasource.SchemaResponse
	ds.Schema(ctx, datasource.SchemaRequest{}, &schemaOut)
	dataOut := datasource.ReadResponse{State: state}
	ds.Read(ctx, datasource.ReadRequest{Config: tfsdk.Config{Raw: state.Raw, Schema: schemaOut.Schema}}, &dataOut)
	if !dataOut.Diagnostics.HasError() || !dataOut.State.Raw.Equal(state.Raw) || calls != 3 {
		t.Fatal("data source drift rewrote sealed config or reported success")
	}
}

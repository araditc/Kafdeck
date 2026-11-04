package kafdeck

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func desiredFixture() Desired {
	return Desired{DeploymentID: "prod", OwnerID: "owner", SourceRef: "git", SourceRevision: strings.Repeat("a", 64), ResourceID: "sub", DestinationRef: "dest", SubscriptionState: "Active", EventClasses: []string{"Operational"}, EventTypes: []string{}, ApplyPlanID: "11111111-1111-1111-1111-111111111111", ApplyPlanFingerprint: strings.Repeat("b", 64)}
}
func observedFixture() Observed {
	r := int64(2)
	return Observed{ScopeKind: "Deployment", ScopeID: "prod", Kind: "NotificationSubscription", SchemaVersion: 1, Drift: "InSync", Capability: "Supported", Authorization: "Allowed", DesiredFingerprint: strings.Repeat("a", 64), ObservedAtUTC: time.Now().UTC(), Ownership: "Managed", OwnershipRevision: 1, SubscriptionRevision: &r, SourceInSync: true, OwnershipGeneration: "22222222-2222-2222-2222-222222222222", SourceGeneration: "33333333-3333-3333-3333-333333333333", DeploymentGeneration: "44444444-4444-4444-4444-444444444444", DeploymentID: "prod", ResourceID: "sub", OwnerRef: "owner", SourceRef: "git", Current: &Subscription{"dest", "Active", []string{"Operational"}, []string{}}}
}
func planFixture(state string) Plan {
	d := desiredFixture()
	return Plan{DeploymentGeneration: "44444444-4444-4444-4444-444444444444", PlanID: d.ApplyPlanID, PlanFingerprint: d.ApplyPlanFingerprint, DeploymentID: d.DeploymentID, ResourceKind: "NotificationSubscription", ResourceID: d.ResourceID, OwnerID: d.OwnerID, SourceID: d.SourceRef, SourceRevisionHash: d.SourceRevision, DesiredFingerprint: d.SourceRevision, State: state, Command: "Apply", Desired: Subscription{d.DestinationRef, d.SubscriptionState, d.EventClasses, d.EventTypes}, ExpiresAtUTC: time.Now().Add(time.Minute)}
}
func response(body any) *http.Response {
	b, _ := json.Marshal(body)
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header)}
}
func TestObserveRejectsEachTruthStatusEvenHTTP200(t *testing.T) {
	for _, status := range []string{"Unknown", "Inaccessible", "Conflict", "Unsupported", "Missing", ""} {
		t.Run(status, func(t *testing.T) {
			o := observedFixture()
			o.Drift = status
			c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
				if q.URL.Path != "/api/v1/declarative/observe" || q.Method != "POST" {
					t.Fatal("untyped route")
				}
				return response(o), nil
			})})
			if _, e := c.Observe(context.Background(), desiredFixture(), false); e == nil {
				t.Fatal("accepted unavailable truth")
			}
		})
	}
}
func TestObserveRejectsIncompleteStaleOrCrossScopeProof(t *testing.T) {
	for _, mutate := range []func(*Observed){func(o *Observed) { o.DeploymentID = "other" }, func(o *Observed) { o.ResourceID = "other" }, func(o *Observed) { o.OwnerRef = "other" }, func(o *Observed) { o.SourceRef = "other" }, func(o *Observed) { o.OwnershipGeneration = "" }, func(o *Observed) { o.SourceGeneration = "" }, func(o *Observed) { o.DeploymentGeneration = "" }, func(o *Observed) { o.SourceInSync = false }, func(o *Observed) { o.Ownership = "ObservedOnly" }, func(o *Observed) { o.Current = nil }, func(o *Observed) { o.ObservedAtUTC = time.Now().Add(-time.Hour) }, func(o *Observed) { o.DesiredFingerprint = "" }} {
		o := observedFixture()
		mutate(&o)
		c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) { return response(o), nil })})
		if _, e := c.Observe(context.Background(), desiredFixture(), false); e == nil {
			t.Fatal("accepted bad proof")
		}
	}
}
func TestPendingUnknownAndForeignPlansNeverDispatch(t *testing.T) {
	for _, state := range []string{"AwaitingConfirmation", "AwaitingApproval", "ExecutionUnknown", "Failed", ""} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
				calls++
				if q.Method != "GET" {
					t.Fatal("pending plan dispatched")
				}
				return response(planFixture(state)), nil
			})})
			if _, e := c.Apply(context.Background(), desiredFixture()); e == nil || calls != 1 {
				t.Fatal("pending reported success or retried")
			}
		})
	}
	p := planFixture("Ready")
	p.OwnerID = "foreign"
	c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
		if q.Method != "GET" {
			t.Fatal("foreign plan dispatched")
		}
		return response(p), nil
	})})
	if _, e := c.Apply(context.Background(), desiredFixture()); e == nil {
		t.Fatal("foreign accepted")
	}
}
func TestUnknownAfterDispatchNeverRetriesOrBecomesSuccess(t *testing.T) {
	posts := 0
	c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
		if q.Method == "GET" {
			return response(planFixture("Ready")), nil
		}
		posts++
		d := desiredFixture()
		return response(MutationStatus{d.ApplyPlanID, d.ApplyPlanFingerprint, "executionUnknown"}), nil
	})})
	if _, e := c.Apply(context.Background(), desiredFixture()); e == nil || posts != 1 {
		t.Fatal("unknown reported success or retried")
	}
}
func TestVerifiedOperationRestartObservesWithoutDispatch(t *testing.T) {
	posts := 0
	c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
		if q.Method == "GET" {
			return response(planFixture("AppliedVerified")), nil
		}
		if strings.HasSuffix(q.URL.Path, "/recover") {
			d := desiredFixture()
			return response(MutationStatus{d.ApplyPlanID, d.ApplyPlanFingerprint, "appliedVerified"}), nil
		}
		if q.URL.Path != "/api/v1/declarative/observe" {
			posts++
			t.Fatal("verified operation redispatched")
		}
		return response(observedFixture()), nil
	})})
	if _, e := c.Apply(context.Background(), desiredFixture()); e != nil || posts != 0 {
		t.Fatal(e)
	}
}
func TestHTTPSAndNumericLoopbackOnly(t *testing.T) {
	for _, u := range []string{"http://localhost:8080", "http://api.internal", "http://127.0.0.1.evil.test", "https://user:secret@host", "file:///etc/passwd", "https://host/path", "https://host?x=1"} {
		if _, e := NewClient(u, "secret", nil); e == nil {
			t.Errorf("accepted %s", u)
		}
	}
	for _, u := range []string{"http://127.0.0.1:8080", "http://[::1]:8080", "https://api.internal"} {
		if _, e := NewClient(u, "secret", nil); e != nil {
			t.Error(e)
		}
	}
}

func TestImportBindsObservedOwnershipRevisionAndRejectsRace(t *testing.T) {
	for _, race := range []bool{false, true} {
		calls := 0
		c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
			calls++
			if q.URL.Path == "/api/v1/declarative/observe" {
				return response(observedFixture()), nil
			}
			if q.URL.Path != "/api/v1/declarative/import-check" {
				t.Fatal("unexpected route")
			}
			var r ManifestRequest
			if json.NewDecoder(q.Body).Decode(&r) != nil || r.ExpectedOwnershipRevision != 1 {
				t.Fatal("import missing exact owner CAS")
			}
			if race {
				return &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
			}
			return response(observedFixture()), nil
		})})
		_, e := c.Observe(context.Background(), desiredFixture(), true)
		if (e != nil) != race || calls != 2 {
			t.Fatal("import race accepted or retried", e, calls)
		}
	}
}

func TestVerifiedReplayRejectsRevokedApplyAndOldGeneration(t *testing.T) {
	for _, revoked := range []bool{true, false} {
		c, _ := NewClient("https://example.test", "secret", &http.Client{Transport: roundTrip(func(q *http.Request) (*http.Response, error) {
			if q.Method == "GET" {
				p := planFixture("AppliedVerified")
				if !revoked {
					p.DeploymentGeneration = "99999999-9999-9999-9999-999999999999"
				}
				return response(p), nil
			}
			if strings.HasSuffix(q.URL.Path, "/apply") {
				t.Fatal("verified replay dispatched")
			}
			if strings.HasSuffix(q.URL.Path, "/recover") {
				if revoked {
					return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
				}
				d := desiredFixture()
				return response(MutationStatus{d.ApplyPlanID, d.ApplyPlanFingerprint, "appliedVerified"}), nil
			}
			if revoked {
				t.Fatal("readback accepted revoked apply authority")
			}
			return response(observedFixture()), nil
		})})
		if _, e := c.Apply(context.Background(), desiredFixture()); e == nil {
			t.Fatal("revoked or old generation reported success")
		}
	}
}

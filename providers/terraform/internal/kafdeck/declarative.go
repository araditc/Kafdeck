package kafdeck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
)

const APIVersion = "kafdeck.io/v0.8"

var identifier = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
var hash = regexp.MustCompile(`^[a-f0-9]{64}$`)
var uuid = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

type Subscription struct {
	DestinationRef string   `json:"destinationRef"`
	State          string   `json:"state"`
	EventClasses   []string `json:"eventClasses"`
	EventTypes     []string `json:"eventTypes"`
}
type Desired struct {
	DeploymentID         string   `json:"deployment_id"`
	OwnerID              string   `json:"owner_id"`
	SourceRef            string   `json:"source_ref"`
	SourceRevision       string   `json:"source_revision"`
	ResourceID           string   `json:"resource_id"`
	DestinationRef       string   `json:"destination_ref"`
	SubscriptionState    string   `json:"subscription_state"`
	EventClasses         []string `json:"event_classes"`
	EventTypes           []string `json:"event_types"`
	ApplyPlanID          string   `json:"apply_plan_id"`
	ApplyPlanFingerprint string   `json:"apply_plan_fingerprint"`
}

func (d Desired) Validate() error {
	for _, v := range []string{d.DeploymentID, d.OwnerID, d.SourceRef, d.ResourceID, d.DestinationRef} {
		if !identifier.MatchString(v) || v == "." || v == ".." {
			return errors.New("invalid typed identity")
		}
	}
	if !hash.MatchString(d.SourceRevision) || (!uuid.MatchString(d.ApplyPlanID) && d.ApplyPlanID != "") || (!hash.MatchString(d.ApplyPlanFingerprint) && d.ApplyPlanFingerprint != "") {
		return errors.New("invalid source or plan identity")
	}
	if d.SubscriptionState != "Active" && d.SubscriptionState != "Paused" {
		return errors.New("only Active/Paused subscription states are admitted")
	}
	if len(d.EventClasses) < 1 || len(d.EventClasses) > 5 || len(d.EventTypes) > 32 {
		return errors.New("invalid bounded event filters")
	}
	seen := map[string]bool{}
	for _, v := range d.EventClasses {
		if !slices.Contains([]string{"Operational", "Mutation", "Security", "Slo", "DataQuality"}, v) || seen[v] {
			return errors.New("invalid event classes")
		}
		seen[v] = true
	}
	seen = map[string]bool{}
	for _, v := range d.EventTypes {
		if !identifier.MatchString(v) || seen[v] {
			return errors.New("invalid event types")
		}
		seen[v] = true
	}
	return nil
}
func (d Desired) Manifest() string {
	classes := append([]string{}, d.EventClasses...)
	order := []string{"Operational", "Mutation", "Security", "Slo", "DataQuality"}
	slices.SortFunc(classes, func(a, b string) int { return slices.Index(order, a) - slices.Index(order, b) })
	types := append([]string{}, d.EventTypes...)
	slices.Sort(types)
	v := struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			DeploymentRef string `json:"deploymentRef"`
			Name          string `json:"name"`
			OwnerRef      string `json:"ownerRef"`
		} `json:"metadata"`
		Spec Subscription `json:"spec"`
	}{APIVersion: APIVersion, Kind: "NotificationSubscription", Spec: Subscription{d.DestinationRef, d.SubscriptionState, classes, types}}
	v.Metadata.DeploymentRef = d.DeploymentID
	v.Metadata.Name = d.ResourceID
	v.Metadata.OwnerRef = d.OwnerID
	b, _ := json.Marshal(v)
	return string(b)
}

type ManifestRequest struct {
	Manifest                  string `json:"manifest"`
	Format                    string `json:"format"`
	SourceRef                 string `json:"sourceRef"`
	SourceRevision            string `json:"sourceRevision"`
	ExpectedOwnershipRevision int64  `json:"expectedOwnershipRevision"`
}

func (d Desired) Request() ManifestRequest {
	return ManifestRequest{d.Manifest(), "json", d.SourceRef, d.SourceRevision, 0}
}

type Observed struct {
	ScopeKind            string        `json:"scopeKind"`
	ScopeID              string        `json:"scopeId"`
	Kind                 string        `json:"kind"`
	SchemaVersion        int           `json:"schemaVersion"`
	Drift                string        `json:"drift"`
	Capability           string        `json:"capability"`
	Authorization        string        `json:"authorization"`
	DesiredFingerprint   string        `json:"desiredFingerprint"`
	ObservedAtUTC        time.Time     `json:"observedAtUtc"`
	Ownership            string        `json:"ownership"`
	OwnershipRevision    int64         `json:"ownershipRevision"`
	SubscriptionRevision *int64        `json:"subscriptionRevision"`
	SourceInSync         bool          `json:"sourceInSync"`
	OwnershipGeneration  string        `json:"ownershipGeneration"`
	SourceGeneration     string        `json:"sourceGeneration"`
	DeploymentGeneration string        `json:"deploymentGeneration"`
	DeploymentID         string        `json:"deploymentId"`
	ResourceID           string        `json:"resourceId"`
	OwnerRef             string        `json:"ownerRef"`
	SourceRef            string        `json:"sourceRef"`
	Current              *Subscription `json:"current"`
}
type Plan struct {
	DeploymentGeneration string       `json:"deploymentGeneration"`
	PlanID               string       `json:"planId"`
	PlanFingerprint      string       `json:"planFingerprint"`
	DeploymentID         string       `json:"deploymentId"`
	ResourceKind         string       `json:"resourceKind"`
	ResourceID           string       `json:"resourceId"`
	OwnerID              string       `json:"ownerId"`
	SourceID             string       `json:"sourceId"`
	SourceRevisionHash   string       `json:"sourceRevisionHash"`
	DesiredFingerprint   string       `json:"desiredFingerprint"`
	State                string       `json:"state"`
	Command              string       `json:"command"`
	Desired              Subscription `json:"desired"`
	ExpiresAtUTC         time.Time    `json:"expiresAtUtc"`
}
type MutationStatus struct {
	OperationID string `json:"operationId"`
	PreviewHash string `json:"previewHash"`
	State       string `json:"state"`
}

func (c *Client) request(ctx context.Context, method, path string, body any, out any) error {
	var b []byte
	var e error
	if body != nil {
		b, e = json.Marshal(body)
		if e != nil {
			return errors.New("invalid request")
		}
	}
	u := *c.base
	u.Path = path
	req, e := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(b))
	if e != nil {
		return errors.New("invalid API request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, e := c.http.Do(req)
	if e != nil {
		return errors.New("Kafdeck API transport unavailable; retain state and observe the saved operation before retry")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("Kafdeck API returned HTTP %d; retain prior state", res.StatusCode)
	}
	b, e = io.ReadAll(io.LimitReader(res.Body, 65537))
	if e != nil || len(b) > 65536 {
		return errors.New("Kafdeck response exceeds bounded contract")
	}
	if ValidateJSON(b) != nil {
		return errors.New("invalid bounded Kafdeck response")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if dec.Decode(out) != nil {
		return errors.New("invalid Kafdeck response")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return errors.New("invalid trailing API content")
	}
	return nil
}
func (c *Client) Observe(ctx context.Context, d Desired, importCheck bool) (Observed, error) {
	var o Observed
	if e := d.Validate(); e != nil {
		return o, e
	}
	path := "/api/v1/declarative/observe"
	request := d.Request()
	if importCheck {
		// Observe read permission first, then bind the separately authorized
		// import check to that exact owner revision. A race must conflict.
		prior, e := c.Observe(ctx, d, false)
		if e != nil {
			return o, e
		}
		request.ExpectedOwnershipRevision = prior.OwnershipRevision
		path = "/api/v1/declarative/import-check"
	}
	if e := c.request(ctx, http.MethodPost, path, request, &o); e != nil {
		return o, e
	}
	if o.ScopeKind != "Deployment" || o.ScopeID != d.DeploymentID || o.Kind != "NotificationSubscription" || o.SchemaVersion != 1 || o.Capability != "Supported" || o.Authorization != "Allowed" || o.DeploymentID != d.DeploymentID || o.ResourceID != d.ResourceID || o.OwnerRef != d.OwnerID || o.SourceRef != d.SourceRef || o.DesiredFingerprint != d.SourceRevision || o.ObservedAtUTC.IsZero() || time.Since(o.ObservedAtUTC) > time.Minute || time.Until(o.ObservedAtUTC) > time.Minute {
		return o, errors.New("incomplete, stale or mismatched observation; retain state")
	}
	if o.Drift != "InSync" && o.Drift != "OutOfSync" {
		return o, fmt.Errorf("declarative observation %s; retain prior state", safeStatus(o.Drift))
	}
	if o.Ownership != "Managed" || o.OwnershipRevision < 1 || o.SubscriptionRevision == nil || !o.SourceInSync || !uuid.MatchString(o.OwnershipGeneration) || !uuid.MatchString(o.SourceGeneration) || !uuid.MatchString(o.DeploymentGeneration) || o.Current == nil {
		return o, errors.New("unmanaged or unbound source/owner observation; explicit governed adoption is required")
	}
	current := d
	current.DestinationRef = o.Current.DestinationRef
	current.SubscriptionState = o.Current.State
	current.EventClasses = o.Current.EventClasses
	current.EventTypes = o.Current.EventTypes
	if current.Validate() != nil {
		return o, errors.New("invalid typed current observation; retain state")
	}
	return o, nil
}
func safeStatus(s string) string {
	if slices.Contains([]string{"Unknown", "Inaccessible", "Conflict", "Unsupported", "Missing"}, s) {
		return s
	}
	return "Unknown"
}
func (c *Client) ReadPlan(ctx context.Context, d Desired) (Plan, error) {
	var p Plan
	if !uuid.MatchString(d.ApplyPlanID) || !hash.MatchString(d.ApplyPlanFingerprint) {
		return p, errors.New("explicit immutable apply_plan_id and apply_plan_fingerprint required")
	}
	if e := c.request(ctx, http.MethodGet, "/api/v1/declarative/plans/"+d.ApplyPlanID, nil, &p); e != nil {
		return p, e
	}
	if !strings.EqualFold(p.PlanID, d.ApplyPlanID) || p.PlanFingerprint != d.ApplyPlanFingerprint || p.DeploymentID != d.DeploymentID || p.ResourceID != d.ResourceID || p.ResourceKind != "NotificationSubscription" || p.OwnerID != d.OwnerID || p.SourceID != d.SourceRef || p.SourceRevisionHash != d.SourceRevision || p.DesiredFingerprint != d.SourceRevision || !sameSubscription(p.Desired, Subscription{d.DestinationRef, d.SubscriptionState, d.EventClasses, d.EventTypes}) {
		return p, errors.New("stored plan does not match the exact typed configuration")
	}
	return p, nil
}
func sameSubscription(a, b Subscription) bool {
	return a.DestinationRef == b.DestinationRef && a.State == b.State && sameSet(a.EventClasses, b.EventClasses) && sameSet(a.EventTypes, b.EventTypes)
}
func sameSet(a, b []string) bool {
	a = append([]string{}, a...)
	b = append([]string{}, b...)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}
func (c *Client) Apply(ctx context.Context, d Desired) (Observed, error) {
	var zero Observed
	p, e := c.ReadPlan(ctx, d)
	if e != nil {
		return zero, e
	}
	switch p.State {
	case "AppliedVerified":
		// Verify a positive existing receipt under current authority/generation.
		// Recover never redispatches an effect; unknown outcomes are not sent here.
	case "Ready":
		if p.Command != "Apply" && p.Command != "Adopt" {
			return zero, errors.New("unadmitted plan command")
		}
		if !p.ExpiresAtUTC.After(time.Now()) {
			return zero, errors.New("stored plan expired; explicit re-plan required")
		}
	default:
		return zero, fmt.Errorf("operation %s is %s; not successful; inspect it without redispatch", d.ApplyPlanID, operationStatus(p.State))
	}
	var status MutationStatus
	action := "apply"
	if p.State == "AppliedVerified" {
		action = "recover"
	}
	e = c.request(ctx, http.MethodPost, "/api/v1/declarative/plans/"+d.ApplyPlanID+"/"+action, struct {
		PlanFingerprint string `json:"planFingerprint"`
	}{d.ApplyPlanFingerprint}, &status)
	if e != nil {
		return zero, fmt.Errorf("operation %s: %w; observe its durable status before any further apply", d.ApplyPlanID, e)
	}
	if !strings.EqualFold(status.OperationID, d.ApplyPlanID) || status.PreviewHash != d.ApplyPlanFingerprint || status.State != "appliedVerified" {
		return zero, fmt.Errorf("operation %s is %s; no synchronous success or retry", d.ApplyPlanID, operationStatus(status.State))
	}
	o, e := c.Observe(ctx, d, false)
	if e != nil {
		return zero, e
	}
	if !uuid.MatchString(p.DeploymentGeneration) || p.DeploymentGeneration != o.DeploymentGeneration {
		return zero, errors.New("saved plan deployment generation is stale; retain state")
	}
	return o, nil
}
func operationStatus(s string) string {
	switch strings.ToLower(s) {
	case "awaitingconfirmation":
		return "AwaitingConfirmation"
	case "awaitingapproval":
		return "AwaitingApproval"
	case "executionunknown":
		return "ExecutionUnknown"
	case "ready":
		return "Ready"
	case "appliedverified":
		return "AppliedVerified"
	default:
		return "NotComplete"
	}
}

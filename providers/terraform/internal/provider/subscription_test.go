package provider

import (
	"encoding/json"
	"github.com/araditc/Kafdeck/providers/terraform/internal/kafdeck"
	"strings"
	"testing"
)

func TestExactImportRejectsUnknownDuplicateAndUnboundFields(t *testing.T) {
	d := kafdeck.Desired{DeploymentID: "prod", OwnerID: "owner", SourceRef: "git", SourceRevision: strings.Repeat("a", 64), ResourceID: "sub", DestinationRef: "dest", SubscriptionState: "Active", EventClasses: []string{"Operational"}, EventTypes: []string{}, ApplyPlanID: "11111111-1111-1111-1111-111111111111", ApplyPlanFingerprint: strings.Repeat("b", 64)}
	b, _ := json.Marshal(d)
	if _, e := parseImport(string(b)); e != nil {
		t.Fatal(e)
	}
	for _, s := range []string{string(b) + `{}`, strings.TrimSuffix(string(b), "}") + `,"owner_id":"other"}`, strings.TrimSuffix(string(b), "}") + `,"token":"secret"}`, strings.Replace(string(b), d.ApplyPlanID, "", 1), strings.Replace(string(b), "Active", "Retired", 1)} {
		if _, e := parseImport(s); e == nil {
			t.Fatal("unsafe import accepted")
		}
	}
}

package kafdeck
import "testing"
func TestStoredPlanDenials(t *testing.T){
 h:= "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
 e:=StoredPlanEvidence{PlanID:"plan",PlanFingerprint:h,DesiredFingerprint:h,DeploymentID:"d",ResourceID:"r",InitiatorID:"p",PolicyVersion:"v",CurrentRevision:3,PlannedRevision:3,ExpiresValid:true,AuthorizedDeclarativeApply:true,AuthorizedEffect:true,IndependentApproval:true}
 if ValidateStoredPlan(e,"plan",h,"p")!=nil {t.Fatal("valid evidence")}
 for _,x:=range []StoredPlanEvidence{func()StoredPlanEvidence{z:=e;z.ExpiresValid=false;return z}(),func()StoredPlanEvidence{z:=e;z.AuthorizedEffect=false;return z}(),func()StoredPlanEvidence{z:=e;z.AuthorizedDeclarativeApply=false;return z}(),func()StoredPlanEvidence{z:=e;z.IndependentApproval=false;return z}(),func()StoredPlanEvidence{z:=e;z.CurrentRevision=4;return z}()} {if ValidateStoredPlan(x,"plan",h,"p")==nil{t.Fatal("denied plan passed")}}
}

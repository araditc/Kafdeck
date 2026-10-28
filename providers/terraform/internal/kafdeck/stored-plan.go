package kafdeck
import ("errors";"strings")
var ErrPlanDenied=errors.New("immutable plan authority invalid")
type StoredPlanEvidence struct {
 PlanID,PlanFingerprint,DesiredFingerprint,DeploymentID,ResourceID,InitiatorID,PolicyVersion string
 CurrentRevision,PlannedRevision int64
 ExpiresValid,AuthorizedDeclarativeApply,AuthorizedEffect,IndependentApproval bool
}
func ValidateStoredPlan(e StoredPlanEvidence,planID,planFingerprint,initiator string)error{
 if planID==""||e.PlanID!=planID||e.PlanFingerprint!=planFingerprint||
  e.InitiatorID!=initiator||initiator==""||e.DeploymentID==""||e.ResourceID==""||
  e.PolicyVersion==""||e.CurrentRevision<0||e.CurrentRevision!=e.PlannedRevision||
  !e.ExpiresValid||!e.AuthorizedDeclarativeApply||!e.AuthorizedEffect||
  !e.IndependentApproval||!sha(e.PlanFingerprint)||!sha(e.DesiredFingerprint){
  return ErrPlanDenied
 };return nil
}
func sha(s string)bool{
 if len(s)!=64{return false}
 for _,c:=range s {if !strings.ContainsRune("0123456789abcdef",c){return false}}
 return true
}

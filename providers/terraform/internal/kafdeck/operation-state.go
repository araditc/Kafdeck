package kafdeck

import "errors"

// OperationState is a server-owned durable state, not Terraform success.
type OperationState string
const (
 AwaitingApproval OperationState="AwaitingApproval"
 ExecutionUnknown OperationState="ExecutionUnknown"
 Succeeded OperationState="Succeeded"
 Failed OperationState="Failed"
)
type Operation struct {
 ID string
 State OperationState
 PrincipalID string
}
var ErrNotComplete=errors.New("governed operation not complete")
func ReconcileOperation(op Operation,expectedID,principal string) error {
 if expectedID=="" || principal=="" || op.ID!=expectedID || op.PrincipalID!=principal {
  return ErrNotComplete
 }
 switch op.State {
 case Succeeded:return nil
 case AwaitingApproval,ExecutionUnknown,Failed:return ErrNotComplete
 default:return ErrNotComplete
 }
}

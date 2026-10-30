package kafdeck
import "testing"
func TestUnknownAndAwaitingNeverBecomeSuccess(t *testing.T){
 for _,state:=range []OperationState{AwaitingApproval,ExecutionUnknown,Failed,""}{
  if ReconcileOperation(Operation{ID:"op",PrincipalID:"p",State:state},"op","p")==nil {t.Fatal(state)}
 }
 if ReconcileOperation(Operation{ID:"op",PrincipalID:"p",State:Succeeded},"op","other")==nil {t.Fatal("foreign principal accepted")}
 if ReconcileOperation(Operation{ID:"op",PrincipalID:"p",State:Succeeded},"op","p")!=nil {t.Fatal("completed status rejected")}
}

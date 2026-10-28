package kafdeck
import "errors"
var ErrDestroyDenied=errors.New("destructive lifecycle not admitted")
type DestroyKind string
const (DestroyNone DestroyKind="None";DestroyRetire DestroyKind="Retire";DestroyRelinquish DestroyKind="Relinquish";DestroyDelete DestroyKind="Delete")
type DestroyAdmission struct {Kind DestroyKind;Owned bool;Authorized bool;VerifiedPlan bool;EffectKnownSafe bool;AuditAvailable bool}
func CanDestroy(e DestroyAdmission)error{
 if !e.Owned||!e.Authorized||!e.VerifiedPlan||!e.EffectKnownSafe||!e.AuditAvailable{return ErrDestroyDenied}
 switch e.Kind {case DestroyRetire,DestroyRelinquish:return nil;default:return ErrDestroyDenied}
}

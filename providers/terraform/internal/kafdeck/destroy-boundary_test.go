package kafdeck
import "testing"
func TestDeleteNotGenericLifecycle(t *testing.T){
 e:=DestroyAdmission{Kind:DestroyRetire,Owned:true,Authorized:true,VerifiedPlan:true,EffectKnownSafe:true,AuditAvailable:true}
 if CanDestroy(e)!=nil {t.Fatal("retire denied")}
 for _,x:=range []DestroyAdmission{func()DestroyAdmission{a:=e;a.Kind=DestroyDelete;return a}(),func()DestroyAdmission{a:=e;a.Owned=false;return a}(),func()DestroyAdmission{a:=e;a.VerifiedPlan=false;return a}(),func()DestroyAdmission{a:=e;a.AuditAvailable=false;return a}()}{
 if CanDestroy(x)==nil {t.Fatal("unsafe destroy accepted")}
 }
}

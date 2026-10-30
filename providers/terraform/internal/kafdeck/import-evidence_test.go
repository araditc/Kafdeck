package kafdeck
import "testing"
func TestImportRequiresOwnerSourceAndIndependentGrant(t *testing.T){
 e:=ImportEvidence{DeploymentID:"prod",ResourceID:"sub",RegisteredOwner:"owner",RegisteredSource:"git",PrincipalID:"p",Generation:"g",Fresh:true,AuthorizedRead:true,AuthorizedImport:true}
 check:=func(x ImportEvidence) bool{return ValidateImport(x,"prod","sub","owner","git","p")==nil}
 if !check(e){t.Fatal("valid exact evidence denied")}
 for _,x:=range []ImportEvidence{eWithOwner(e,"other"),eWithSource(e,"other"),func()ImportEvidence{v:=e;v.AuthorizedImport=false;return v}(),func()ImportEvidence{v:=e;v.CompetingClaim=true;return v}(),func()ImportEvidence{v:=e;v.Terminal=true;return v}(),func()ImportEvidence{v:=e;v.Fresh=false;return v}()}{
  if check(x){t.Fatal("invalid import accepted")}
 }
}
func eWithOwner(e ImportEvidence,s string)ImportEvidence{e.RegisteredOwner=s;return e}
func eWithSource(e ImportEvidence,s string)ImportEvidence{e.RegisteredSource=s;return e}

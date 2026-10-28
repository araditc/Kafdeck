package kafdeck

import (
 "errors"
 "strings"
)
var ErrImportDenied=errors.New("import not authorized")
type ImportEvidence struct {
 DeploymentID string
 ResourceID string
 RegisteredOwner string
 RegisteredSource string
 PrincipalID string
 Generation string
 Fresh bool
 AuthorizedRead bool
 AuthorizedImport bool
 CompetingClaim bool
 Terminal bool
}
func ValidateImport(e ImportEvidence, deployment,resource,owner,source,principal string) error {
 if !e.Fresh || !e.AuthorizedRead || !e.AuthorizedImport || e.CompetingClaim || e.Terminal ||
  e.Generation=="" || strings.TrimSpace(e.Generation)!=e.Generation ||
  deployment=="" || resource=="" || owner=="" || source=="" || principal=="" ||
  e.DeploymentID!=deployment || e.ResourceID!=resource ||
  e.RegisteredOwner!=owner || e.RegisteredSource!=source || e.PrincipalID!=principal {
  return ErrImportDenied
 }
 return nil
}

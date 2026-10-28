// Package kafdeck contains a fail-closed HTTP-only client for future Terraform resources.
// It is not registered as a Terraform provider and does not create mutation permissions.
package kafdeck

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "net/http"
 "net/url"
 "strings"
 "time"
)

type DriftStatus string
const (
 InSync DriftStatus = "InSync"
 OutOfSync DriftStatus = "OutOfSync"
 Missing DriftStatus = "Missing"
 Unknown DriftStatus = "Unknown"
 Inaccessible DriftStatus = "Inaccessible"
 Conflict DriftStatus = "Conflict"
 Unsupported DriftStatus = "Unsupported"
)

type Observation struct {
 Status DriftStatus `json:"status"`
 DeploymentID string `json:"deploymentId"`
 ResourceID string `json:"resourceId"`
 Revision int64 `json:"revision"`
 OwnershipGeneration string `json:"ownershipGeneration"`
}
type Client struct { base *url.URL; http *http.Client; token string }
var ErrUnavailable = errors.New("authoritative observation unavailable")
func NewClient(endpoint, token string, httpClient *http.Client) (*Client,error) {
 u,e:=url.Parse(endpoint)
 if e!=nil || u==nil || u.Scheme!="https" || u.Host=="" || u.User!=nil || u.RawQuery!="" || u.Fragment!="" || u.Path!="" {return nil,errors.New("HTTPS origin required")}
 if strings.TrimSpace(token)=="" {return nil,errors.New("token required")}
 if httpClient==nil {httpClient=&http.Client{Timeout:10*time.Second}}
 return &Client{base:u,http:httpClient,token:token},nil
}
// ReadExact requires an explicit versioned backend read path supplied by the admitted API.
// An unresolved W68 API path must never be guessed or silently synthesized.
func (c *Client) ReadExact(ctx context.Context, apiPath, deployment, resource string) (Observation,error) {
 var zero Observation
 if c==nil || c.base==nil || !strings.HasPrefix(apiPath,"/api/v1/") || strings.Contains(apiPath,"..") || strings.ContainsAny(apiPath,"?#") || strings.Contains(apiPath,"//") || deployment=="" || resource=="" {
  return zero,ErrUnavailable
 }
 u:=*c.base
 u.Path=apiPath
 req,e:=http.NewRequestWithContext(ctx,http.MethodGet,u.String(),nil)
 if e!=nil{return zero,ErrUnavailable}
 req.Header.Set("Authorization","Bearer "+c.token)
 req.Header.Set("Accept","application/json")
 res,e:=c.http.Do(req)
 if e!=nil{return zero,ErrUnavailable}
 defer res.Body.Close()
 if res.StatusCode!=http.StatusOK{return zero,ErrUnavailable}
 dec:=json.NewDecoder(io.LimitReader(res.Body,64*1024))
 dec.DisallowUnknownFields()
 var found Observation
 if dec.Decode(&found)!=nil{return zero,ErrUnavailable}
 var extra any
 if dec.Decode(&extra)!=io.EOF{return zero,ErrUnavailable}
 if found.DeploymentID!=deployment || found.ResourceID!=resource || found.Revision<0 || found.OwnershipGeneration=="" {
  return zero,ErrUnavailable
 }
 switch found.Status {
 case InSync,OutOfSync:
  return found,nil
 case Missing,Unknown,Inaccessible,Conflict,Unsupported:
  return zero,fmt.Errorf("%w: %s; retain prior Terraform state",ErrUnavailable,found.Status)
 default:
  return zero,ErrUnavailable
 }
}

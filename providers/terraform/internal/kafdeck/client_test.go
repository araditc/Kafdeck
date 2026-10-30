package kafdeck

import (
 "context"
 "errors"
 "io"
 "net/http"
 "strings"
 "testing"
)

type roundTrip func(*http.Request)(*http.Response,error)
func (r roundTrip) RoundTrip(q *http.Request)(*http.Response,error){return r(q)}
func TestFailClosedTypedReads(t *testing.T){
 cases:=[]struct{body string; ok bool}{
  {`{"status":"InSync","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,true},
  {`{"status":"Unknown","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"Inaccessible","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"Conflict","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"Unsupported","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"Missing","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"InSync","deploymentId":"other","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"InSync","deploymentId":"prod","resourceId":"sub","revision":2}`,false},
  {`{"status":"InSync","deploymentId":"prod","resourceId":"sub","ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"InSync","deploymentId":"prod","resourceId":"sub","revision":null,"ownershipGeneration":"owner-gen"}`,false},
  {`{"status":"InSync","deploymentId":"prod","resourceId":"sub","revision":2,"ownershipGeneration":"owner-gen"}`+strings.Repeat(" ",65536)+`{}`,false},
 }
 for _,tc:=range cases{
  transport:=roundTrip(func(req *http.Request)(*http.Response,error){
   if req.Method!="GET" || req.Header.Get("Authorization")!="Bearer secret" {t.Error("unexpected request identity")}
   return &http.Response{StatusCode:200,Body:io.NopCloser(strings.NewReader(tc.body)),Header:make(http.Header)},nil
  })
  c,err:=NewClient("https://kafdeck.test","secret",&http.Client{Transport:transport})
  if err!=nil {t.Fatal(err)}
  _,err=c.ReadExact(context.Background(),"/api/v1/declarative/observations","prod","sub")
  if (err==nil)!=tc.ok {t.Errorf("body %s: err=%v",tc.body,err)}
 }
}
func TestRejectBadOriginAndNoAuthority(t *testing.T){
 for _,u:=range []string{"http://server","https://user:pass@server","https://server/path","https://server?x=1"} {
  if _,e:=NewClient(u,"token",nil);e==nil {t.Errorf("accepted %q",u)}
 }
 c,_:=NewClient("https://kafdeck.test","secret",&http.Client{Transport:roundTrip(func(*http.Request)(*http.Response,error){t.Fatal("unexpected network call");return nil,errors.New("not reached")})})
 if _,e:=c.ReadExact(context.Background(),"/admin/sql","prod","sub");e==nil {t.Fatal("unadmitted path")}
 if _,e:=c.ReadExact(context.Background(),"/api/v1/../admin","prod","sub");e==nil {t.Fatal("path traversal")}
}

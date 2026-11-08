{{- define "kafdeck.selector" -}}
app.kubernetes.io/name: kafdeck
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{- define "kafdeck.metadata" -}}
name: {{ printf "%s-kafdeck" .Release.Name }}
labels:
  {{- include "kafdeck.selector" . | nindent 2 }}
  app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

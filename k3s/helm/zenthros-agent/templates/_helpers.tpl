{{- define "zenthros-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "zenthros-agent.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "zenthros-agent.name" . | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "zenthros-agent.labels" -}}
app.kubernetes.io/name: {{ include "zenthros-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: agent
{{- end -}}

{{- define "zenthros-agent.selectorLabels" -}}
app.kubernetes.io/component: agent
{{- end -}}

{{- define "zenthros-agent.enrollmentImage" -}}
{{- default .Values.agent.image .Values.enrollment.image -}}
{{- end -}}

{{- define "zenthros-agent.imagePullSecrets" -}}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

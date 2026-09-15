{{- define "krabka-schema-registry.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "krabka-schema-registry.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Name of the headless Service that gives each pod its DNS record. The
"-headless" suffix is 9 characters, so the base name keeps 54 of the 63
characters a DNS label allows. Without that reserve a long release name
produces a Service that the API server rejects. */}}
{{- define "krabka-schema-registry.headlessName" -}}
{{- printf "%s-headless" (include "krabka-schema-registry.fullname" . | trunc 54 | trimSuffix "-") -}}
{{- end -}}

{{- define "krabka-schema-registry.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "krabka-schema-registry.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "krabka-schema-registry.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "krabka-schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: schema-registry
{{- end -}}

{{- define "krabka-schema-registry.selectorLabels" -}}
app.kubernetes.io/name: {{ include "krabka-schema-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

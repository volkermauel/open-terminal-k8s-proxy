{{/*
Expand the name of the release.
*/}}
{{- define "name" .Values.name | default "open-terminal-k8s-proxy" -}}
{{- define "fullname" .Values.fullname | default (printf "%s-%s" .Values.name .Values.version |replace "-" "_") -}}

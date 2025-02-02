apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
  labels:
    app: {{.Name}}
    app-type: dubbo
spec:
  replicas: {{.Replicas}}
  revisionHistoryLimit: {{.Revisions}}
  selector:
    matchLabels:
      app: {{.Name}}
      app-type: dubbo
  template:
    metadata:
      labels:
        app: {{.Name}}
        app-type: dubbo{{if .UseProm}}
      #helm-charts 配置  https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/path: /management/prometheus
        prometheus.io/port: "18081"{{end}}
    spec:{{if .ServiceAccount}}
      serviceAccountName: {{.ServiceAccount}}{{end}}
      containers:
      - name: {{.Name}}
        image: {{.Image}}
        env:
          - name: DUBBO_CTL_VERSION
            value: 0.0.1{{if .Zookeeper}}
          - name: zookeeper.address
            value: {{.Zookeeper}}
          - name: ZOOKEEPER_ADDRESS
            value: {{.Zookeeper}}{{end}}{{if .Nacos}}
          - name: nacos.address
            value: {{.Nacos}}
          - name: NACOS_ADDRESS
            value: {{.Nacos}}{{end}}
        {{if .ImagePullPolicy}}imagePullPolicy: {{.ImagePullPolicy}}
        {{end}}ports:
        - containerPort: {{.Port}}
          name: dubbo
          protocol: TCP{{if .UseProm}}
        - containerPort: 18081
          name: metrics
          protocol: TCP{{end}}
        readinessProbe:
          tcpSocket:
            port: {{.Port}}
          initialDelaySeconds: 5
          periodSeconds: 10
        livenessProbe:
          tcpSocket:
            port: {{.Port}}
          initialDelaySeconds: 15
          periodSeconds: 20
        resources:
          requests:
            cpu: {{.RequestCpu}}m
            memory: {{.RequestMem}}Mi
          limits:
            cpu: {{.LimitCpu}}m
            memory: {{.LimitMem}}Mi
      {{if .Secret}}imagePullSecrets:
      - name: {{.Secret}}{{end}}
---

apiVersion: v1
kind: Service
metadata:
  name: {{.Name}}-svc
  namespace: {{.Namespace}}
spec:
  ports:
  {{if .UseNodePort}}- nodePort: {{.NodePort}}
    port: {{.Port}}
    protocol: TCP
    targetPort: {{.TargetPort}}
  type: NodePort{{else}}- port: {{.Port}}
    targetPort: {{.TargetPort}}{{end}}{{if .UseProm}}
  - port: 18081
    targetPort: 18081{{end}}
  selector:
    app: {{.Name}}

---

apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{.Name}}-hpa-c
  namespace: {{.Namespace}}
  labels:
    app: {{.Name}}-hpa-c
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{.Name}}
  minReplicas: {{.MinReplicas}}
  maxReplicas: {{.MaxReplicas}}
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 80

---

apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{.Name}}-hpa-m
  namespace: {{.Namespace}}
  labels:
    app: {{.Name}}-hpa-m
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{.Name}}
  minReplicas: {{.MinReplicas}}
  maxReplicas: {{.MaxReplicas}}
  metrics:
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80

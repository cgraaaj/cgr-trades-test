# ArgoCD Setup for Opt-STK Data Collection

This directory contains ArgoCD Application manifests for deploying the opt-stk data collection service using GitOps principles.

## Overview

The CI/CD pipeline now follows a GitOps approach:
1. **GitHub Actions**: Builds, tests, and pushes container images
2. **Manifest Updates**: Updates Kubernetes manifests with new image tags
3. **ArgoCD**: Automatically deploys updated manifests to Kubernetes

## Files

- `opt-stk-data-collection-app.yaml`: ArgoCD Application and Project manifests
- `README.md`: This setup guide

## Prerequisites

1. **ArgoCD Installation**: ArgoCD must be installed in your Kubernetes cluster
2. **Git Repository Access**: ArgoCD needs access to your Git repository
3. **Container Registry Access**: Cluster needs access to your Harbor registry

## Quick Setup

### 1. Update Repository URL

Edit `opt-stk-data-collection-app.yaml` and update the repository URL:

```yaml
source:
  repoURL: https://github.com/your-username/cgr-trades.git  # Update this
```

### 2. Configure Namespace (Optional)

Update the target namespace if different from `default`:

```yaml
destination:
  server: https://kubernetes.default.svc
  namespace: your-namespace  # Update this
```

### 3. Deploy ArgoCD Application

```bash
# Apply the ArgoCD application
kubectl apply -f argocd/opt-stk-data-collection-app.yaml

# Verify application is created
kubectl get applications -n argocd
```

### 4. Access ArgoCD UI

```bash
# Get ArgoCD admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# Port forward to access UI (if not using ingress)
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Open browser to https://localhost:8080
```

## Configuration Options

### Sync Policy

The application is configured with automatic sync:

```yaml
syncPolicy:
  automated:
    prune: true      # Remove resources not in Git
    selfHeal: true   # Sync when cluster state differs from Git
```

To disable automatic sync:

```yaml
syncPolicy:
  # Remove the automated section
  syncOptions:
    - Validate=true
```

### Custom Project

If you want to use a custom ArgoCD project instead of `default`:

1. Update the project reference in the Application:
   ```yaml
   spec:
     project: opt-stk-project  # Use custom project
   ```

2. Apply the project first:
   ```bash
   kubectl apply -f argocd/opt-stk-data-collection-app.yaml
   ```

### Ignore Differences

The application ignores certain fields that may change during runtime:

```yaml
ignoreDifferences:
  - group: batch
    kind: CronJob
    jsonPointers:
      - /spec/jobTemplate/spec/template/spec/containers/0/env
      - /spec/schedule
```

## Workflow Integration

The GitHub Actions workflow automatically:

1. **Builds** the container image with SHA-based tags
2. **Updates** the kustomization.yaml with the new image tag
3. **Commits** the changes back to the repository
4. **Triggers** ArgoCD to sync the new deployment

### Image Tag Strategy

Images are tagged with git commit SHA for better traceability:

```
registry.cgraaaj.in/stock-x/opt-stk-data-collection:sha-abcd1234
```

ArgoCD will automatically pull and deploy these new images.

## Monitoring and Troubleshooting

### Check Application Status

```bash
# Get application status
kubectl get application opt-stk-data-collection -n argocd

# Get detailed status
kubectl describe application opt-stk-data-collection -n argocd

# View application in ArgoCD CLI
argocd app get opt-stk-data-collection
```

### View Sync History

```bash
# View sync history
argocd app history opt-stk-data-collection

# View last sync details
argocd app get opt-stk-data-collection --show-operation
```

### Manual Sync

```bash
# Trigger manual sync
argocd app sync opt-stk-data-collection

# Sync with force (if needed)
argocd app sync opt-stk-data-collection --force
```

### View Application Logs

```bash
# View ArgoCD application controller logs
kubectl logs -n argocd -l app.kubernetes.io/name=argocd-application-controller

# View deployed CronJob
kubectl get cronjobs
kubectl describe cronjob opt-stk-data-collection-job
```

## Common Issues and Solutions

### 1. Application Stuck in Progressing State

```bash
# Check application events
kubectl describe application opt-stk-data-collection -n argocd

# Check resource status
argocd app get opt-stk-data-collection --show-params
```

### 2. Image Pull Errors

Ensure the cluster has access to your Harbor registry:

```bash
# Check if image pull secret exists
kubectl get secrets

# Create image pull secret if needed
kubectl create secret docker-registry harbor-secret \
  --docker-server=registry.cgraaaj.in \
  --docker-username=your-username \
  --docker-password=your-password
```

### 3. Permission Issues

Check ArgoCD has proper RBAC permissions:

```bash
# Check ArgoCD service account
kubectl get clusterrole argocd-application-controller
kubectl get clusterrolebinding argocd-application-controller
```

### 4. Sync Out of Sync

Force refresh the application:

```bash
# Refresh application
argocd app refresh opt-stk-data-collection

# Hard refresh (re-read from Git)
argocd app refresh opt-stk-data-collection --hard
```

## Security Considerations

### Repository Access

For private repositories, configure repository credentials in ArgoCD:

```bash
# Add repository via CLI
argocd repo add https://github.com/your-username/cgr-trades.git \
  --username your-username \
  --password your-token

# Or via UI: Settings → Repositories
```

### Image Registry Access

Ensure proper image pull secrets are configured:

```yaml
# In your deployment spec
spec:
  template:
    spec:
      imagePullSecrets:
        - name: harbor-secret
```

### RBAC

Limit ArgoCD permissions to necessary namespaces and resources:

```yaml
# Custom project with restricted permissions
namespaceResourceWhitelist:
  - group: 'batch'
    kind: CronJob
  - group: ''
    kind: ConfigMap
  - group: ''
    kind: Secret
```

## Best Practices

### 1. Environment Separation

Use different ArgoCD applications for different environments:

```yaml
# staging-app.yaml
metadata:
  name: opt-stk-data-collection-staging
spec:
  source:
    targetRevision: develop  # Use develop branch
    path: k8s-cronjob/opt-stk-data-collection/overlays/staging

# production-app.yaml
metadata:
  name: opt-stk-data-collection-production
spec:
  source:
    targetRevision: HEAD  # Use main branch
    path: k8s-cronjob/opt-stk-data-collection/overlays/production
```

### 2. Resource Management

Use ArgoCD resource quotas and limits:

```yaml
spec:
  syncPolicy:
    syncOptions:
      - CreateNamespace=true
      - ApplyOutOfSyncOnly=true
```

### 3. Monitoring

Set up alerts for application health:

```yaml
# Add health check annotations
metadata:
  annotations:
    argocd.argoproj.io/health.batch.CronJob: |
      hs = {}
      if obj.status.active ~= nil and obj.status.active > 0 then
        hs.status = "Progressing"
        hs.message = "CronJob is active"
      else
        hs.status = "Healthy"
        hs.message = "CronJob is ready"
      end
      return hs
```

## Rollback Procedures

### Rolling Back to Previous Version

```bash
# View application history
argocd app history opt-stk-data-collection

# Rollback to specific revision
argocd app rollback opt-stk-data-collection <revision-id>

# Or sync to specific git commit
argocd app sync opt-stk-data-collection --revision <git-commit-sha>
```

### Emergency Procedures

1. **Disable Auto-Sync**: Remove automated sync policy
2. **Manual Intervention**: Use kubectl to directly modify resources
3. **Git Revert**: Revert problematic commits in Git repository

## Integration with CI/CD

The pipeline automatically updates manifests when new images are built. Monitor the process:

1. **GitHub Actions**: Check workflow completion
2. **Git Commits**: Verify manifest updates are committed
3. **ArgoCD Sync**: Confirm ArgoCD picks up changes
4. **Deployment**: Verify resources are updated in cluster

## Support and Maintenance

### Regular Tasks

1. **Monitor ArgoCD Health**: Check application and sync status
2. **Review Sync History**: Investigate any failed syncs
3. **Update ArgoCD**: Keep ArgoCD version updated
4. **Clean Up**: Remove old application revisions

### Getting Help

For ArgoCD-related issues:

1. Check ArgoCD documentation: https://argo-cd.readthedocs.io/
2. Review application events and logs
3. Use ArgoCD CLI for debugging
4. Check GitHub Actions workflow logs for manifest updates

---

**Note**: This GitOps approach provides better auditability, rollback capabilities, and separation of concerns between CI (build) and CD (deploy) processes. 
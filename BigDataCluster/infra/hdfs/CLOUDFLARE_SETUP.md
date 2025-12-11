# HDFS + Cloudflare Tunnel Setup Guide

This guide explains how to expose your HDFS WebHDFS API publicly using Cloudflare Tunnel.

## Prerequisites

- Docker and Docker Compose installed
- Cloudflare account (free tier works)
- A domain managed by Cloudflare (for named tunnel)

---

## Quick Tunnel Setup (Two Tunnels: NameNode + DataNode)

WebHDFS requires access to both NameNode (for metadata) and DataNode (for actual data).
We create two separate Cloudflare tunnels.

### Start HDFS with Quick Tunnels

```powershell
cd BigDataCluster/infra/hdfs

# Start HDFS + both Cloudflare tunnels
docker compose --profile quick-tunnel -f hdfs-compose.yml up -d

# Get the NameNode tunnel URL
docker logs cloudflared-hdfs-namenode 2>&1 | Select-String "trycloudflare.com"

# Get the DataNode tunnel URL  
docker logs cloudflared-hdfs-datanode 2>&1 | Select-String "trycloudflare.com"
```

You'll see output like:
```
https://random-namenode-words.trycloudflare.com
https://random-datanode-words.trycloudflare.com
```

### Use in Colab Notebook

```python
# Configuration
HDFS_NAMENODE_URL = "https://random-namenode-words.trycloudflare.com"
HDFS_DATANODE_URL = "https://random-datanode-words.trycloudflare.com"
HDFS_USER = "root"
```

> ⚠️ **Note**: Quick tunnel URLs change every restart. Use named tunnels for persistent URLs.

---

## Option 2: Named Tunnel (Production)

Persistent URL with your own domain.

### Step 1: Install Cloudflared

```powershell
# Windows
winget install Cloudflare.cloudflared

# Or download from:
# https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/
```

### Step 2: Authenticate

```powershell
cloudflared tunnel login
```

This opens a browser to authenticate with Cloudflare.

### Step 3: Create a Tunnel

```powershell
cloudflared tunnel create hdfs-tunnel
```

Output:
```
Tunnel credentials written to C:\Users\YourUser\.cloudflared\<TUNNEL-ID>.json
Created tunnel hdfs-tunnel with id <TUNNEL-ID>
```

### Step 4: Copy Credentials

```powershell
# Copy the credentials file to the cloudflared folder
copy "$env:USERPROFILE\.cloudflared\<TUNNEL-ID>.json" ".\cloudflared\"
```

### Step 5: Update Config

Edit `cloudflared/config.yml`:

```yaml
tunnel: hdfs-tunnel
credentials-file: /etc/cloudflared/<TUNNEL-ID>.json

ingress:
  - hostname: hdfs.yourdomain.com
    service: http://namenode:9870
  - service: http_status:404
```

### Step 6: Route DNS

```powershell
cloudflared tunnel route dns hdfs-tunnel hdfs.yourdomain.com
```

### Step 7: Start HDFS with Named Tunnel

```powershell
cd BigDataCluster/infra/hdfs

# Start with named tunnel profile
docker-compose --profile named-tunnel up -d

# Check status
docker logs cloudflared-hdfs
```

### Use in Notebook

```python
HDFS_NAMENODE_HOST = "hdfs.yourdomain.com"
HDFS_NAMENODE_PORT = 443
USE_HTTPS = True
```

---

## Verify Connection

### Check HDFS Health

```powershell
# Quick tunnel
curl https://random-words.trycloudflare.com/webhdfs/v1/?op=LISTSTATUS

# Named tunnel  
curl https://hdfs.yourdomain.com/webhdfs/v1/?op=LISTSTATUS
```

### From Python

```python
from hdfs import InsecureClient

# Connect via Cloudflare tunnel
client = InsecureClient('https://hdfs.yourdomain.com', user='root')

# List root directory
print(client.list('/'))
```

---

## Docker Commands Reference

```powershell
# Start HDFS only (no tunnel)
docker-compose up -d

# Start with quick tunnel (testing)
docker-compose --profile quick-tunnel up -d

# Start with named tunnel (production)
docker-compose --profile named-tunnel up -d

# View logs
docker logs namenode
docker logs cloudflared-hdfs-quick
docker logs cloudflared-hdfs

# Stop all
docker-compose --profile quick-tunnel --profile named-tunnel down

# Restart cloudflared only
docker restart cloudflared-hdfs
```

---

## Security Best Practices

1. **Use Cloudflare Access** for authentication:
   - Go to Cloudflare Zero Trust Dashboard
   - Create an Application → Self-hosted
   - Add access policies (email, IP whitelist, etc.)

2. **HDFS Permissions**: Keep `dfs.permissions.enabled=false` only for development

3. **Read-Only Access**: Create a read-only HDFS user for public access

4. **Monitor Access**: Enable Cloudflare analytics to monitor requests

---

## Troubleshooting

### Tunnel Not Connecting

```powershell
# Check cloudflared logs
docker logs cloudflared-hdfs -f

# Verify credentials file exists
ls ./cloudflared/
```

### WebHDFS Errors

```powershell
# Check namenode is healthy
docker exec namenode curl http://localhost:9870/webhdfs/v1/?op=LISTSTATUS

# Check from cloudflared container
docker exec cloudflared-hdfs curl http://namenode:9870/webhdfs/v1/?op=LISTSTATUS
```

### DNS Not Resolving

```powershell
# Verify DNS record
nslookup hdfs.yourdomain.com

# Re-route DNS
cloudflared tunnel route dns hdfs-tunnel hdfs.yourdomain.com
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Internet                              │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Cloudflare Edge                            │
│              (hdfs.yourdomain.com:443)                       │
│                   ┌─────────────┐                            │
│                   │ Zero Trust  │ ← Optional Auth            │
│                   │   Access    │                            │
│                   └─────────────┘                            │
└─────────────────────────┬───────────────────────────────────┘
                          │ Encrypted Tunnel
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Docker Network                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │  cloudflared │───▶│   namenode   │◀──▶│   datanode   │   │
│  │   (tunnel)   │    │  (WebHDFS)   │    │   (data)     │   │
│  │              │    │   :9870      │    │   :9864      │   │
│  └──────────────┘    └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

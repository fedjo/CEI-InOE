# CEI-InOE Ansible Deployment

Ansible playbook for deploying CEI-InOE to Intel NUC devices.

## Prerequisites

- **Control machine**: Ansible 2.14+ with Python 3.9+
- **Target NUC**: Ubuntu Server 22.04 or 24.04 with SSH access
- **Network**: Control machine can reach NUC via SSH
- **Git access**: SSH key or HTTPS credentials for the CEI-InOE repository

## Quick Start

### 1. Install Ansible Collections

```bash
cd ansible
ansible-galaxy collection install -r requirements.yml
```

### 2. Configure Inventory

Edit `inventory/hosts.yml` and set the correct IP address and SSH user for your NUC:

```yaml
cei-nuc-01:
  ansible_host: 192.168.1.50  # Change to your NUC IP
  ansible_user: ubuntu         # Change if using different user
```

### 3. Configure Variables

Edit `group_vars/nuc.yml` and adjust:

- `cei_inoe_repo_url`: Repository URL (SSH or HTTPS)
- `cei_inoe_version`: Git branch or tag to deploy
- `cei_inoe_cors_origins`: Allowed CORS origins (include NUC IP)
- Connector settings: poll intervals, lookback days, etc.

### 4. Set Secrets

Encrypt the vault file with your secrets:

```bash
# Create encrypted vault (first time)
ansible-vault create group_vars/nuc/vault.yml
```

Or edit existing vault:

```bash
ansible-vault edit group_vars/nuc/vault.yml
```

Required secrets:

```yaml
vault_cei_inoe_db_password: "secure-db-password"
vault_cei_inoe_api_key: "secure-api-key"
vault_cei_inoe_airbeld_email: "your-email@example.com"
vault_cei_inoe_airbeld_password: "airbeld-password"
# Add other connector credentials as needed
```

### 5. Deploy

Run the playbook:

```bash
ansible-playbook -i inventory/hosts.yml playbooks/deploy.yml \
  --ask-become-pass \
  --ask-vault-pass
```

The playbook will:
1. Install Docker Engine and Compose plugin
2. Install Git and Python dependencies
3. Create deployment directories
4. Clone the CEI-InOE repository
5. Render environment configuration
6. Start the Docker Compose stack
7. Wait for services to be healthy

### 6. Verify Deployment

After successful deployment, verify the services:

```bash
# Check health endpoint
curl http://NUC_IP:8000/health

# Check readiness
curl http://NUC_IP:8000/ready

# View API documentation
# Open in browser: http://NUC_IP:8000/docs

# View Grafana dashboards
# Open in browser: http://NUC_IP:3000
```

Test authenticated API access:

```bash
curl -H "X-API-Key: YOUR_API_KEY" http://NUC_IP:8000/api/v1/datasources
```

## Updating the Deployment

To update to a newer version:

```bash
# Update to latest main branch
ansible-playbook -i inventory/hosts.yml playbooks/deploy.yml \
  --ask-become-pass \
  --ask-vault-pass

# Or deploy a specific version
ansible-playbook -i inventory/hosts.yml playbooks/deploy.yml \
  -e cei_inoe_version=v1.2.3 \
  --ask-become-pass \
  --ask-vault-pass
```

## Managing Services on the NUC

SSH into the NUC and use Docker Compose commands:

```bash
cd /opt/cei-inoe

# View running services
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml ps

# View logs
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml logs -f

# View specific service logs
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml logs -f api
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml logs -f ingestor

# Restart services
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml restart

# Stop services
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml down

# Start services
docker compose -f docker-compose.new.yaml -f docker-compose.override.yml up -d
```

## Directory Structure

```
ansible/
├── README.md                          # This file
├── requirements.yml                   # Ansible collection dependencies
├── inventory/
│   └── hosts.yml                      # NUC inventory
├── group_vars/
│   ├── nuc.yml                        # Non-secret variables
│   └── nuc/
│       └── vault.yml                  # Encrypted secrets
├── templates/
│   ├── cei-inoe.env.j2                # Application environment file
│   └── docker-compose.override.yml.j2 # Compose override template
└── playbooks/
    └── deploy.yml                     # Main deployment playbook
```

## Troubleshooting

### SSH Connection Issues

If Ansible cannot connect to the NUC:

1. Verify SSH access: `ssh ubuntu@NUC_IP`
2. Check inventory file has correct IP and user
3. Ensure SSH key is in ssh-agent or use `--ask-pass`

### Docker Permission Errors

If you see Docker permission errors after deployment:

```bash
# On the NUC, log out and back in to apply group membership
exit
ssh ubuntu@NUC_IP
```

### Service Health Check Fails

If the health check times out:

1. SSH into the NUC
2. Check service status: `docker compose ps`
3. Check logs: `docker compose logs api`
4. Verify network connectivity
5. Check if port 8000 is accessible

### Database Connection Issues

If services fail to connect to PostgreSQL:

1. Check database logs: `docker compose logs postgres`
2. Verify `.env` file has correct `DB_DSN` and `DATABASE_URL`
3. Ensure migrations completed: `docker compose logs migrations`

## Security Notes

- The `vault.yml` file should always be encrypted with `ansible-vault`
- Never commit unencrypted secrets to version control
- Change default passwords before production deployment
- Consider restricting PostgreSQL port to localhost only
- Use strong API keys for production deployments

## Next Steps

For production deployments, consider adding:

- HTTPS reverse proxy (Nginx or Caddy)
- Tailscale for secure remote access
- UFW firewall configuration
- Automated backups for PostgreSQL
- Monitoring and alerting
- Log aggregation

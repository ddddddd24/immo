# Oracle Cloud Always Free — Guide de déploiement

VPS ARM gratuit à vie : 4 OCPU + 24 Go RAM + 200 Go SSD. Setup 90 min copy-paste.

## 1. Création compte Oracle Cloud

1. https://signup.cloud.oracle.com/
2. **Country** : France · **Name** : matche la CB
3. Mail jamais utilisé sur Oracle (sinon ban auto)
4. **Home Region** (DÉFINITIF, pas de revert) — par ordre de préférence :
   - **Marseille (eu-marseille-1)** — souvent dispo
   - **Frankfurt (eu-frankfurt-1)** — populaire mais grande capacité
   - **Amsterdam** — souvent saturé en ARM, éviter
   - **Paris (eu-paris-1)** — petit datacenter
5. CB Visa/MC. Pré-auto 1€ remboursée. Si rejet → ne pas re-soumettre 5× (= ban)
6. SMS validation FR
7. Activer 2FA : avatar → My Profile → Multi-Factor Authentication

**Trial 300$** : ignore, expire dans 30j. Tu utilises **Always Free** uniquement (badge vert).

## 2. Provision VM ARM

### SSH key (PowerShell Windows)
```powershell
ssh-keygen -t ed25519 -f $env:USERPROFILE\.ssh\oracle_immo -C "immo-bot"
cat $env:USERPROFILE\.ssh\oracle_immo.pub  # copie cette sortie
```

### Console Oracle → Compute → Instances → Create
- **Name** : `immo-bot`
- **Image** : Canonical Ubuntu 22.04
- **Shape** : Ampere → `VM.Standard.A1.Flex` → 4 OCPU + 24 GB RAM
  - Si "Out of host capacity" → réessaye toutes les 30 min, ou descends à 2 OCPU/12 GB
- **Networking** : Assign public IPv4 ✅
- **SSH keys** : colle ta clé publique
- **Boot volume** : 50 GB

Wait 2 min (icône verte). Note l'IP publique.

### Réserver l'IP (gratuit, fixe au reboot)
Instance → Attached VNICs → VNIC → IPv4 → Edit → Reserved Public IP → Create

### Ouvrir ports (Security List)
Instance → Subnet → Default Security List → Add Ingress Rule :
- TCP 22, 80, 443 depuis `0.0.0.0/0`
- ⚠️ Ne PAS exposer 5000 directement (Cloudflare Tunnel ou Tailscale plus tard)

## 3. Setup serveur

### Premier SSH
```powershell
ssh -i $env:USERPROFILE\.ssh\oracle_immo ubuntu@<IP_PUBLIQUE>
```

### Update + outils
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl wget build-essential software-properties-common \
    sqlite3 ca-certificates gnupg ufw htop tmux unzip
```

### Python 3.11
```bash
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev python3-pip
python3.11 --version
```

### UFW + fix iptables Oracle
```bash
sudo ufw allow 22/tcp && sudo ufw allow 80/tcp && sudo ufw allow 443/tcp
sudo ufw --force enable
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save
```

### Deps système Camoufox ARM
```bash
sudo apt install -y \
    libgtk-3-0 libgbm1 libxss1 libasound2 libnss3 libxrandr2 libatk-bridge2.0-0 \
    libdrm2 libxcomposite1 libxdamage1 libpangocairo-1.0-0 libpango-1.0-0 \
    libxshmfence1 libxkbcommon0 libwayland-client0 libwayland-egl1 libwayland-cursor0 \
    libdbus-glib-1-2 libxt6 libx11-xcb1 fonts-liberation xvfb
```

## 4. Transfert code

### Via GitHub (recommandé)
```powershell
# Sur Windows
gh repo create immo-bot --private --source=. --push
```
```bash
# Sur Oracle
git clone https://<USER>:<TOKEN>@github.com/<USER>/immo-bot.git immo
cd immo
```

### Transfert sécurisé .env + DB (jamais via git)
```powershell
scp -i $env:USERPROFILE\.ssh\oracle_immo "D:\...\immo\.env" ubuntu@<IP>:/home/ubuntu/immo/.env
scp -i $env:USERPROFILE\.ssh\oracle_immo "D:\...\immo\data\bot.db" ubuntu@<IP>:/home/ubuntu/immo/data/bot.db
```
```bash
chmod 600 /home/ubuntu/immo/.env
mkdir -p /home/ubuntu/immo/data && chmod 700 /home/ubuntu/immo/data
```

## 5. Install Python deps

```bash
cd ~/immo
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel setuptools
pip install -r requirements.txt
playwright install firefox chromium
playwright install-deps
```

### Camoufox ARM (peut casser)
```bash
python -m camoufox fetch
```

Si erreur `No release found for linux aarch64` :
```bash
cd /tmp
# Check https://github.com/daijro/camoufox/releases pour la dernière version
wget https://github.com/daijro/camoufox/releases/download/v<VERSION>/camoufox-<VERSION>-<X>-lin.aarch64.zip
unzip camoufox-*.zip -d camoufox-bin
mkdir -p ~/.cache/camoufox
mv camoufox-bin/* ~/.cache/camoufox/
chmod +x ~/.cache/camoufox/camoufox
```

Test smoke :
```bash
python -c "
import asyncio
from camoufox.async_api import AsyncCamoufox
async def t():
    async with AsyncCamoufox(headless=True) as b:
        p = await b.new_page()
        await p.goto('https://example.com')
        print(await p.title())
asyncio.run(t())
"
```
→ doit afficher `Example Domain`

## 6. systemd service (auto-restart + démarrage au boot)

```bash
sudo tee /etc/systemd/system/immo-bot.service > /dev/null <<'EOF'
[Unit]
Description=Immo Telegram Bot
After=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/immo
EnvironmentFile=/home/ubuntu/immo/.env
ExecStart=/home/ubuntu/immo/.venv/bin/python /home/ubuntu/immo/main.py
Restart=always
RestartSec=10
MemoryMax=4G
CPUQuota=200%

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable immo-bot
sudo systemctl start immo-bot
sudo systemctl status immo-bot
journalctl -u immo-bot -f
```

Service dashboard séparé :
```bash
sudo tee /etc/systemd/system/immo-dashboard.service > /dev/null <<'EOF'
[Unit]
Description=Immo Dashboard
After=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/immo
EnvironmentFile=/home/ubuntu/immo/.env
ExecStart=/home/ubuntu/immo/.venv/bin/python /home/ubuntu/immo/dashboard.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload && sudo systemctl enable --now immo-dashboard
```

## 7. Dashboard accessible depuis le S21+

### Option A : Tailscale (le plus simple, 0 config DNS)
```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```
Installe l'app Tailscale sur ton phone, login même compte. Le serveur a une IP `100.x.y.z` accessible UNIQUEMENT depuis tes devices.
```bash
sudo ufw allow in on tailscale0 to any port 5000
```
Dashboard : `http://100.x.y.z:5000` depuis ton phone.

### Option B : Cloudflare Tunnel (avec domaine + HTTPS)
```bash
curl -L --output cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb
sudo dpkg -i cloudflared.deb
cloudflared tunnel login
cloudflared tunnel create immo
cloudflared tunnel route dns immo dashboard.<tondomaine>.com

cat > ~/.cloudflared/config.yml <<EOF
tunnel: <UUID>
credentials-file: /home/ubuntu/.cloudflared/<UUID>.json
ingress:
  - hostname: dashboard.<tondomaine>.com
    service: http://localhost:5000
  - service: http_status:404
EOF

sudo cloudflared service install
sudo systemctl start cloudflared && sudo systemctl enable cloudflared
```

## 8. Cookbook erreurs Camoufox ARM

| Erreur | Fix |
|---|---|
| `No release found for linux aarch64` | Download manuel depuis github.com/daijro/camoufox/releases |
| `cannot find -lasound` | `sudo apt install libasound2` |
| `Failed to launch: ENOENT camoufox` | `chmod +x ~/.cache/camoufox/camoufox` |
| `Missing X server or $DISPLAY` | Force `headless=True`, ou `xvfb-run python main.py` |
| `error while loading shared libraries: libgtk-3.so.0` | Re-run section 3.5 + `playwright install-deps` |

## 9. Validation post-deploy

```bash
sudo systemctl status immo-bot   # active (running)
journalctl -u immo-bot -n 100    # pas de stacktrace
# Sur Telegram → /start (réponse immédiate)
# /campagne studapart (test cheap)
sqlite3 ~/immo/data/bot.db "SELECT source, COUNT(*) FROM listings GROUP BY source;"
```

Si `/start` ne répond pas :
- Token `.env` ? `grep TELEGRAM /home/ubuntu/immo/.env`
- Bot Windows toujours allumé ? Stop-le sinon `terminated by other getUpdates`

## 10. Anti-reclaim Always Free

Oracle reclaime les VMs idle (<20% CPU + pas de réseau + pas de login pendant 7j). Bot qui poll en continu = jamais idle. Ceinture+bretelles :
```bash
crontab -e
# Ajoute :
*/10 * * * * timeout 5 sh -c 'yes > /dev/null' >/dev/null 2>&1
```

## Commandes du quotidien

```bash
# Update du code
cd ~/immo && git pull && sudo systemctl restart immo-bot

# Logs live
journalctl -u immo-bot -f

# Backup DB depuis Windows
scp -i $env:USERPROFILE\.ssh\oracle_immo \
    ubuntu@<IP>:/home/ubuntu/immo/data/bot.db \
    "D:\Backups\bot-$(Get-Date -Format yyyyMMdd).db"
```

**Coût : 0 €/mois à vie**.

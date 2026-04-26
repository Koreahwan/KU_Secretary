# systemd user units (WSL / Linux)

KU_Secretary's existing background jobs are wired through `cli_launchd.py`,
which is macOS-only. On WSL or Linux, run the same jobs as **systemd user
units** instead.

These are sample unit files only — `kus systemd install-…` CLI helpers
(equivalent to `kus launchd install-…`) are tracked separately as Phase 6.5.

## Prerequisites

```bash
# Ensure the user systemd manager is running.
loginctl enable-linger "$USER"
systemctl --user daemon-reload
```

For WSL, also enable `systemd` in `/etc/wsl.conf` (`[boot]` section,
`systemd=true`) and restart WSL.

## Available units

| Unit | Schedule | Purpose |
|------|----------|---------|
| `ku-secretary-uclass-poller.{service,timer}` | every 60 min | UClass + KUPID sync (incl. KUPID SSO timetable when `KUPID_SSO_TIMETABLE_ENABLED=true`) |
| `ku-secretary-weather-sync.{service,timer}` | every 30 min | weather + Seoul air-quality snapshot |
| `ku-secretary-briefings.{service,timer}` | every 15 min | morning/evening Telegram briefing dispatch |
| `ku-secretary-telegram-listener.service` | always-on | Telegram long-poll listener |

## Install

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/ku-secretary-*.service ~/.config/systemd/user/
cp deploy/systemd/ku-secretary-*.timer  ~/.config/systemd/user/

systemctl --user daemon-reload
systemctl --user enable --now ku-secretary-uclass-poller.timer
systemctl --user enable --now ku-secretary-weather-sync.timer
systemctl --user enable --now ku-secretary-briefings.timer
systemctl --user enable --now ku-secretary-telegram-listener.service
```

Replace `/path/to/KUSecretary` (used in `EnvironmentFile=` and `WorkingDirectory=`)
and `/path/to/KU_Secretary/.venv/bin/kus` (used in `ExecStart=`) with your
actual install paths before installing. Tip — one-shot rewrite:

```bash
sed -i \
  -e "s|%h/KUSecretary|$HOME/KUSecretary|g" \
  -e "s|/path/to/KU_Secretary/.venv/bin/kus|$HOME/All_Projects/KU_Secretary/.venv/bin/kus|g" \
  ~/.config/systemd/user/ku-secretary-*.service
```

## Status

```bash
systemctl --user list-timers | grep ku-secretary
journalctl --user-unit ku-secretary-uclass-poller -n 50 --no-pager
```

## Uninstall

```bash
systemctl --user disable --now ku-secretary-uclass-poller.timer
systemctl --user disable --now ku-secretary-telegram-listener.service
rm ~/.config/systemd/user/ku-secretary-*.{service,timer}
systemctl --user daemon-reload
```

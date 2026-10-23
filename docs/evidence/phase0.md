# Phase 0 — Host Prerequisites Evidence

Generated: 2026-10-24 (IST)

---

## Step 0 — Machine Report

### Command: `Get-CimInstance Win32_OperatingSystem`
```
Caption     : Microsoft Windows 11 Home Single Language
BuildNumber : 26200
Version     : 10.0.26200
```

### Command: `Get-CimInstance Win32_ComputerSystem`
```
Total RAM (GB) : 31.7
Logical CPUs   : 12
```

### Command: `Get-PSDrive -PSProvider FileSystem`
```
Name  Root  FreeGB  TotalGB
----  ----  ------  -------
C     C:\    8.35   207.02
D     D:\   47.25   198.36
```

### Command: `systeminfo | Select-String "Hyper-V|Virtua"`
```
Virtualization-based security: Status: Running
                                     Base Virtualization Support
                                     APIC Virtualization
Hyper-V Requirements: A hypervisor has been detected.
```

### Command: `dism /online /get-featureinfo /featurename:VirtualMachinePlatform`
```
State : Enabled
```
*(Feature was enabled via: `dism /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart` followed by reboot)*

### Command: `winget --version`
```
v1.29.380
```

### Command: `wsl --status` (before install)
```
Default Version: 2
WSL2 is not supported with your current machine configuration.
Please enable the "Virtual Machine Platform" optional component...
```

### Command: `docker --version` (before install)
```
NOT FOUND
```

### Elevation check
```
Running as Administrator: True
```

---

## Step 1 — WSL2 and Docker Desktop Installation

### Command: `wsl --install --no-distribution`
```
The operation completed successfully.
```

### Command: `wsl --set-default-version 2`
```
For information on key differences with WSL 2 please visit https://aka.ms/wsl2
The operation completed successfully.
```

### Command: `winget install -e --id Docker.DockerDesktop --accept-package-agreements --accept-source-agreements`
```
Found Docker Desktop [Docker.DockerDesktop] Version 4.96.2
Downloading https://desktop.docker.com/win/main/amd64/244353/Docker%20Desktop%20Installer.exe
Successfully verified installer hash
Starting package install...
Successfully installed
```

### Command: `docker --version` (after install + PATH refresh)
```
Docker version 29.9.2, build a82e528
```

### Command: `docker compose version`
```
Docker Compose version v5.6.1
```

### Command: `wsl --version`
```
WSL version:    2.6.1.0
Kernel version: 6.6.87.2-1
WSLg version:   1.0.66
MSRDC version:  1.2.6353
Direct3D version: 1.611.1-81528511
DXCore version: 10.0.26100.1-240331-1435.ge-release
Windows version: 10.0.26200.9711
```

### Command: `docker info` (WSL2 backend verification)
```
 Context:       desktop-linux
 Server Version: 29.9.2
 Kernel Version: 6.6.87.2-microsoft-standard-WSL2
 OSType:        linux
 CPUs:          6
 Total Memory:  7.757GiB
```
WSL2 backend confirmed: kernel string contains `microsoft-standard-WSL2`.

### Command: `docker run --rm hello-world`
```
Hello from Docker!
This message shows that your installation appears to be working correctly.
[... full hello-world output ...]
```

### Command: `docker image rm hello-world`
```
Untagged: hello-world:latest
Deleted: sha256:11a3eb1597e864c51b3f56733cb2e8a90bf0c284bfc05f9444d81c558bc848aa
```

---

## Step 2 — .wslconfig

No prior `.wslconfig` existed; created fresh.

**Chosen values** (RAM=31.7 GB → memory=8GB; CPUs=12 → processors=6):

### File: `%USERPROFILE%\.wslconfig`
```ini
[wsl2]
memory=8GB
processors=6
swap=4GB

[experimental]
sparseVhd=true
autoMemoryReclaim=gradual
```

Applied via: `wsl --shutdown` + Docker Desktop restart.

### Verification — `docker info` after .wslconfig applied
```
 CPUs:         6
 Total Memory: 7.757GiB
```
Limits confirmed active (7.757 GiB ≈ 8 GB, 6 CPUs).

---

## Step 3 — Docker Disk Image

### VHDX location (after move to D:)
```
D:\DockerDesktopWSL\disk\docker_data.vhdx   Size: 1.564 GB
D:\DockerDesktopWSL\main\ext4.vhdx          Size: 0.094 GB
```
Confirmed via: settings-store.json → CustomWslDistroDir: D:\DockerDesktopWSL

### Drive free space at end of setup
```
Name  FreeGB  TotalGB
----  ------  -------
C      9.99   207.02
D     45.59   198.36
```

---

## Step 4 — Workspace

- Workspace: `D:\Downloads\BDA_Lab_ESE\bda-ids\`
- `git init` completed: `Initialized empty Git repository`
- `.gitignore` written (data/staging/, .env, *.vhdx, __pycache__/, .ipynb_checkpoints/)
- `docs/evidence/` created
- `scripts/cleanup.ps1` created

---

## Free space on C: at end of Phase 0

```
C: FreeGB: 9.99 GB   (after Docker VHDX moved to D:)
D: FreeGB: 45.59 GB
```

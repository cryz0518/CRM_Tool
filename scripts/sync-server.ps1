# 将本地代码同步到固定服务器并更新；密码由 OpenSSH 交互询问，不保存到文件。
param([switch]$UploadOnly)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$archive = Join-Path ([System.IO.Path]::GetTempPath()) ('crm-code-' + [guid]::NewGuid().ToString('N') + '.tar.gz')
$remoteArchive = '/opt/CRM_Tool/backups/' + [System.IO.Path]::GetFileName($archive)
try {
    # 配置、凭据、运行数据、历史备份和开发缓存不参与日常代码同步。
    tar -czf $archive --exclude=.git --exclude=.codegraph --exclude=.env --exclude=.env.local --exclude='.env.*' --exclude=.wecom-cli-credentials --exclude=log --exclude=backups --exclude=.venv --exclude=__pycache__ --exclude=.pytest_cache --exclude=.mypy_cache --exclude=.ruff_cache --exclude=build --exclude=dist -C $projectRoot .
    if ($LASTEXITCODE -ne 0) { throw '代码打包失败' }
    scp $archive "root@8.153.164.200:$remoteArchive"
    if ($LASTEXITCODE -ne 0) { throw '上传失败；未执行服务器更新' }
    # 覆盖前保存服务器现有代码；解压再次忽略 .env 和 .env.*，保留服务器配置与数据库卷。
    # shortcut: 归档覆盖不删除旧文件，更新包含删除或重命名时需人工核对旧文件。
    $backupName = 'backups/code-before-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.tar.gz'
    $remoteCommand = "set -eu; cd /opt/CRM_Tool; umask 077; tar --exclude=./backups --exclude=./log --exclude=./.wecom-cli-credentials --exclude=./.env --exclude=./.env.local -czf $backupName .; tar --no-same-permissions --exclude=.env --exclude='.env.*' -xzf '$remoteArchive' -C /opt/CRM_Tool; chgrp --reference=.wecom-cli-credentials employee.csv; chmod 640 employee.csv"
    if (-not $UploadOnly) { $remoteCommand += '; bash scripts/server-update.sh' }
    ssh root@8.153.164.200 $remoteCommand
    if ($LASTEXITCODE -ne 0) { throw '服务器同步或更新失败；请查看服务器 log 目录' }
} finally {
    # 仅删除本次生成且位于系统临时目录的单个归档。
    if (Test-Path -LiteralPath $archive) { Remove-Item -LiteralPath $archive }
}

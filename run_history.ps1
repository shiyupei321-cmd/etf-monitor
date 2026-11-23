# ETF 历史记录任务入口（计划任务触发：复盘 15:05 / 盘中 10:00、14:00、14:30）
# 参数透传：盘中任务传 --intraday；复盘任务不传参
# 注意：本文件必须 UTF-8 BOM 编码，否则 Windows PowerShell 5.1 读取中文路径乱码
$ErrorActionPreference = "Stop"
Set-Location "D:\1claude程序\ETF定投监控"
$logDir = Join-Path $PWD "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir ("history_" + (Get-Date -Format "yyyyMMdd") + ".log")
python -X utf8 history.py @args *>> $log
exit $LASTEXITCODE

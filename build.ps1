$ErrorActionPreference = 'Stop'
Set-Location -Path (Split-Path -Parent $MyInvocation.MyCommand.Path)

python -m pip install --upgrade pip
python -m pip install pyinstaller

python -m PyInstaller --noconfirm --clean --onefile --name "EPPortfolioViewer" --add-data "templates;templates" --add-data "static;static" launcher.py

$desktop = [Environment]::GetFolderPath('Desktop')
Copy-Item -Path "dist\EPPortfolioViewer.exe" -Destination (Join-Path $desktop "EPPortfolioViewer.exe") -Force


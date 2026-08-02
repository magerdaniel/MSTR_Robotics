<#
.SYNOPSIS
    Bootstrap a mstr_robotics working folder: create a venv, install the package
    from PyPI, and copy the bundled utils/ tree up into the project.

    Automates steps 1, 2 and 4 of the install guide in README.md.

.PARAMETER ProjectDir
    Where the project lives. Created if missing. Defaults to the current directory.

.PARAMETER Extras
    Optional-dependency set. "all,dev" is the full machine; see README.md for the
    smaller combinations (rag, redis, azure, servers).

.PARAMETER Version
    Pin a specific release, e.g. "0.5.11". Empty means latest.

.EXAMPLE
    .\bootstrap.ps1
    .\bootstrap.ps1 -ProjectDir C:\Python\my_mstr_project
    .\bootstrap.ps1 -Extras "redis,dev" -Version 0.5.11

.NOTES
    Keep ProjectDir short. The package nests reference material under
    site-packages\mstr_robotics\utils\, and OSI_production carries 50-character
    dashboard filenames; a deep project path pushes the install past the Windows
    260-character MAX_PATH limit and pip fails with "No such file or directory".
    Either use a short path such as C:\Python\<project>, or enable long paths:
    New-ItemProperty -Path HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem `
        -Name LongPathsEnabled -Value 1 -PropertyType DWORD -Force
#>
param(
    [string]$ProjectDir = (Get-Location).Path,
    [string]$Python     = "python",
    [string]$Extras     = "all,dev",
    [string]$VenvName   = ".venv",
    [string]$Version    = ""
)

$ErrorActionPreference = "Stop"

$ProjectDir = [System.IO.Path]::GetFullPath($ProjectDir)
$venv = Join-Path $ProjectDir $VenvName
$py   = Join-Path $venv "Scripts\python.exe"

$spec = "MSTR-Robotics-magerdaniel"
if ($Extras  -ne "") { $spec = "$spec[$Extras]" }
if ($Version -ne "") { $spec = "$spec==$Version" }

New-Item -ItemType Directory -Force -Path $ProjectDir | Out-Null

Write-Host "==> venv          $venv"
& $Python -m venv $venv

Write-Host "==> installing    $spec"
& $py -m pip install --upgrade pip --quiet
& $py -m pip install $spec

# Resolve site-packages directly rather than importing mstr_robotics: the current
# directory is on sys.path, so an import would find a sibling source checkout
# instead of the freshly installed package.
Write-Host "==> locating utils/"
$utils = & $py -c "import sysconfig,pathlib;print(pathlib.Path(sysconfig.get_paths()['purelib'])/'mstr_robotics'/'utils')"

Write-Host "==> copying into  $ProjectDir"
Get-ChildItem -Path $utils -Force | ForEach-Object {
    Copy-Item -Path $_.FullName -Destination $ProjectDir -Recurse -Force
    Write-Host "    $($_.Name)"
}

# pip byte-compiles utils/custom_code (it is declared as a package), so a
# __pycache__ rides along in the copy. Drop it -- it is not reference material.
Get-ChildItem -Path $ProjectDir -Filter __pycache__ -Recurse -Directory -Force |
    Where-Object { $_.FullName -notlike "$venv*" } |
    Remove-Item -Recurse -Force

# The package resolves config/user_d.yml by walking up from the working directory,
# so the notebooks must run with ProjectDir as root -- not from inside .venv.
Write-Host ""
Write-Host "Done."
Write-Host "  activate :  $venv\Scripts\Activate.ps1"
Write-Host "  next     :  fill in config\*.yml (copy each *.example.* first), then run notebooks\00_setup.ipynb"

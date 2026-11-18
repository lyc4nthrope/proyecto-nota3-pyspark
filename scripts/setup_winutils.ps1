<#
.SYNOPSIS
    Instala winutils.exe y hadoop.dll para que PySpark funcione en Windows.

.DESCRIPTION
    Spark en Windows necesita dos binarios de Hadoop para leer y escribir
    archivos: winutils.exe (operaciones de permisos de shell) y hadoop.dll
    (librería nativa que resuelve NativeIO$Windows.access0). Sin ellos,
    escribir Parquet falla con UnsatisfiedLinkError o con
    "Could not locate executable ... winutils.exe in HADOOP_HOME".

    El script crea %LOCALAPPDATA%\hadoop\bin (no requiere administrador),
    descarga los binarios de Hadoop 3.3.6, verifica su SHA-256 contra los
    valores fijados aquí y define HADOOP_HOME y PATH a nivel de usuario.

    PySpark 4.3.0 trae el cliente de Hadoop 3.5.0; la interfaz nativa que
    usa winutils/hadoop.dll es estable en toda la serie 3.x.

.EXAMPLE
    .\scripts\setup_winutils.ps1
#>
$ErrorActionPreference = 'Stop'

$baseUrl = 'https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin'
$expected = @{
    'winutils.exe' = '496A591EB1E67DF2A620F710D529BA6DDFE1C19149E6647CC4E320BB0EFD8553'
    'hadoop.dll'   = 'D7AB36A68518748CEF142BE2DA5069B4C763C2CD764C1D2E6AC48C7200405BE3'
}

$hadoopHome = Join-Path $env:LOCALAPPDATA 'hadoop'
$binDir = Join-Path $hadoopHome 'bin'
New-Item -ItemType Directory -Force -Path $binDir | Out-Null

foreach ($name in $expected.Keys) {
    $target = Join-Path $binDir $name
    if (-not (Test-Path $target)) {
        Write-Host "Descargando $name ..."
        Invoke-WebRequest -Uri "$baseUrl/$name" -OutFile $target -UseBasicParsing
    }
    $actual = (Get-FileHash -Path $target -Algorithm SHA256).Hash
    if ($actual -ne $expected[$name]) {
        throw "${name}: SHA-256 $actual no coincide con el esperado $($expected[$name]). Elimina $target y vuelve a ejecutar."
    }
    Write-Host ("OK  {0}  {1}" -f $name, $actual)
}

[Environment]::SetEnvironmentVariable('HADOOP_HOME', $hadoopHome, 'User')
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not $userPath) { $userPath = '' }
if ($userPath -notlike "*$binDir*") {
    [Environment]::SetEnvironmentVariable('Path', "$binDir;$userPath", 'User')
}

Write-Host ''
Write-Host "HADOOP_HOME = $hadoopHome"
Write-Host 'Listo. Abre una TERMINAL NUEVA y comprueba con: pytest -q'

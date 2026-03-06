@echo off
setlocal

set "NODE_DIR=%~dp0.tools\node\v24.14.0\node-v24.14.0-win-x64"
set "PATH=%NODE_DIR%;%PATH%"

call "%NODE_DIR%\npm.cmd" %*


@echo off
for /f "tokens=*" %%i in ('docker ps -q -f "name=mtusi2026lab4-api-"') do (
    echo Stopping API replica: %%i
    docker stop %%i
    exit /b 0
)
echo No API replicas found.
exit /b 1
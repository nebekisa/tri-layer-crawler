@echo off
echo ========================================
echo Tri-Layer Crawler - Docker Commands
echo ========================================
echo.
echo 1. Build images
echo 2. Start services
echo 3. Stop services
echo 4. View logs
echo 5. Run crawler
echo 6. Connect to database
echo 7. Check status
echo 8. Clean everything
echo.
set /p choice="Enter choice (1-8): "

if "%choice%"=="1" docker-compose build
if "%choice%"=="2" docker-compose up -d postgres api
if "%choice%"=="3" docker-compose down
if "%choice%"=="4" docker-compose logs -f
if "%choice%"=="5" docker-compose --profile crawler run --rm crawler
if "%choice%"=="6" docker-compose exec postgres psql -U crawler_user -d tri_layer_crawler
if "%choice%"=="7" docker-compose ps
if "%choice%"=="8" docker-compose down -v

pause
@echo off
echo ============== 开始同步数据到GitHub ==============
git add .
git commit -m "更新网易云数据"
git push origin main
echo ============== 数据同步完成！==============
echo 等待1-2分钟后，Streamlit链接会自动更新最新数据
pause
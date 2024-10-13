@echo off
setlocal enabledelayedexpansion

REM Files to check
set FILES=Badges.xml Comments.xml PostHistory.xml PostLinks.xml Posts.xml Tags.xml Users.xml Votes.xml

REM Chunk size in bytes (512 MB)
set CHUNK_SIZE=536870912

for %%F in (%FILES%) do (
    echo Checking for char in %%F...
    python check_char_in_xml.py data\%%F
    echo.
)

echo All files have been checked.
pause

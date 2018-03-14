@echo off

set url=
set user=
set password=
set operation=export
set schema=GEO
set path=.

"%JAVA_HOME%\bin\java" -Xmx2G -jar bin/csv-pumper-0.0.1.jar %url% %user% %password% %operation% %schema% %path%

pause

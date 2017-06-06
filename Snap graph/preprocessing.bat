setlocal

set imageinputfolder=D:\ifgi\Copernicus\SnapAutomation\Input\
set imageoutputfolder=D:\ifgi\Copernicus\SnapAutomation\Output\

for /f "tokens=*" %%a in ('dir /b "%imageinputfolder%"') do (
	echo File: %imageinputfolder%%%a
	call gpt PreProcess.xml -Pfilename="%imageinputfolder%%%a" -Poutputfilename="%imageoutputfolder%%%a.dim"
	echo Output File: %imageoutputfolder%%%a.dim
)
setlocal

set imageinputfolder=D:\ifgi\Copernicus\SnapAutomation\Input\

call aws configure set AWS_ACCESS_KEY_ID "AKIAIUYF3M326UZNPSWA"
call aws configure set AWS_SECRET_ACCESS_KEY "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"
call aws configure set default.region eu-central-1
call cd %imageinputfolder% 
call aws s3 sync s3://team-b-test-bucket1 .
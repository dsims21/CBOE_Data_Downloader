#Daniel Sims
#2019

#Script Requirements:
#AWS Tools for Powershell
#AWS CLI

#Set permissions and import necessary libraries
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned  
import-module "C:\Program Files (x86)\AWS Tools\PowerShell\AWSPowerShell\AWSPowerShell.psd1"
Set-AWSCredential -ProfileName S3access
Initialize-AWSDefaults

#Sets a class of connection information to interate through
class InfoClass {
    [string]$S3SubFolder #S3://[Folder]/
	[string]$S3BucketName #[Name] only
	[string]$localTarget #Folder for saved/extracted/extracing files
	[string]$FTPSubFolder #/Subscription/[order]/[item]/
	[string]$fileNamePrefix #Exact file name before date
	[bool]$partitioned #If S3/Athena require partitions
	[string]$athenaTable #Query table Athena uses
	[string]$friendlyName #For Text message header
}

#Populates information into [Options Trades]
$UnderlyingOptionsInfo = [InfoClass]::new() 
$UnderlyingOptionsInfo.S3SubFolder = 's3://underlying-options-partitioned/'
$UnderlyingOptionsInfo.S3BucketName = 'underlying-options-partitioned'
$UnderlyingOptionsInfo.localTarget = 'C:\FTPTest\'
$UnderlyingOptionsInfo.FTPSubFolder = '/subscriptions/order_000009281/item_000012246/'
$UnderlyingOptionsInfo.fileNamePrefix = 'UnderlyingOptionsTrades_'
$UnderlyingOptionsInfo.partitioned = $true
$UnderlyingOptionsInfo.athenaTable = 'underlyingoptions_partitioned'
$UnderlyingOptionsInfo.friendlyName = '[Options Trades]'

#Populates information into [Options EOD w/Calcs]
$OptionsEODInfo = [InfoClass]::new()
$OptionsEODInfo.S3SubFolder = 's3://underlying-options-eod-with-calcs-partitioned/'
$OptionsEODInfo.S3BucketName = 'underlying-options-eod-with-calcs-partitioned'
$OptionsEODInfo.localTarget = 'C:\FTP_Options_EOD\'
$OptionsEODInfo.FTPSubFolder = '/subscriptions/order_000005792/item_000008076/'
$OptionsEODInfo.fileNamePrefix = 'UnderlyingOptionsEODCalcs_'
$OptionsEODInfo.partitioned = $true
$OptionsEODInfo.athenaTable = 'underlyingoptionseodwcalcs_partitioned'
$OptionsEODInfo.friendlyName = '[Options EOD w/Calcs]'

#Populates information into [Equity EOD]
$EquityEODInfo = [InfoClass]::new() 
$EquityEODInfo.S3SubFolder = 's3://underlying-equity-eod/'
$EquityEODInfo.S3BucketName = 'underlying-equity-eod'
$EquityEODInfo.localTarget = 'C:\FTP_Equity_EOD\'
$EquityEODInfo.FTPSubFolder = '/subscriptions/order_000005616/item_000007863/'
$EquityEODInfo.fileNamePrefix = 'UnderlyingEOD_'
$EquityEODInfo.partitioned = $false
$EquityEODInfo.athenaTable = ''
$EquityEODInfo.friendlyName = '[Equity EOD]'

##Populates information into [Equity Interval]
$EquityIntervalInfo = [InfoClass]::new() 
$EquityIntervalInfo.S3SubFolder = 's3://underlying-equity-interval/'
$EquityIntervalInfo.S3BucketName = 'underlying-equity-interval'
$EquityIntervalInfo.localTarget = 'C:\FTP_Equity_Interval\'
$EquityIntervalInfo.FTPSubFolder = '/subscriptions/order_000007670/item_000010282/'
$EquityIntervalInfo.fileNamePrefix = 'UnderlyingIntervals_60sec_'
$EquityIntervalInfo.partitioned = $true
$EquityIntervalInfo.athenaTable = 'equityinterval'
$EquityIntervalInfo.friendlyName = '[Equity Interval]'

##Populates information into [Open-Close]
$OpenCloseInfo = [InfoClass]::new() 
$OpenCloseInfo.S3SubFolder = 's3://open-close-partitioned/'
$OpenCloseInfo.S3BucketName = 'open-close-partitioned'
$OpenCloseInfo.localTarget = 'C:\FTP_Open_Close\'
$OpenCloseInfo.FTPSubFolder = '/subscriptions/order_000009341/item_000012314/'
$OpenCloseInfo.fileNamePrefix = 'OpenClose_'
$OpenCloseInfo.partitioned = $true
$OpenCloseInfo.athenaTable = 'openclose'
$OpenCloseInfo.friendlyName = '[Open-Close]'

#<<<< Any newly purchased data can be entered as a new instance of the InfoClass here.

#Builds an array containing all above objects
$InfoArray = @()
$InfoArray += $UnderlyingOptionsInfo, $OptionsEODInfo, $EquityEODInfo, $EquityIntervalInfo, $OpenCloseInfo

#Iterates through the array of objects
foreach ($info in $InfoArray){
 
##################################################    Builds AWS List   ################################################################################

#Instantiates a list
$AWSlist = New-Object 'System.Collections.Generic.List[String]'

#Brings S3 data into an array/list (unknown) in its native form
$filelist = aws s3 ls $info.S3SubFolder --recursive

#Just in case theres a problem connecting to S3, we dont want to proceed because doing so would cause the download of all FTP files. 
if($filelist -eq $null){
	#Log the error.
	$theDate = get-date

	if (!(Test-Path "C:\FTPTest\error.txt"))
	{
		New-Item -path C:\FTPTest -name error.txt -type "file" -value "$theDate : Could not connect to S3. Please debug."
	}
	else
	{
		Add-Content -path C:\FTPTest\error.txt -value "$theDate : Could not connect to S3. Please debug."
	}
	
	exit
}

[regex]$AWSrx='(' + $info.fileNamePrefix + '.*)(?=.csv)'

foreach ($file in $filelist){

	#If the item in the list contains a search term (from the file name) then keep it. 
	#Some S3 buckets contain folders that would return bad matches. 
	if ($file -match ($info.fileNamePrefix)) {     
		$AWSlist.Add($AWSrx.Match($file.ToString()))
		}
}

#$AWSlist

##################################################    Buids FTP List   ################################################################################

#FTP Credentials and Paths
$user1='xxx'
$pass1='xxx'
$local_target1 = $info.localTarget
$ftp_uri1='ftp://ftp.datashop.livevol.com'
$subfolder1=$info.FTPSubFolder 

##Instantiates a list
$FTPlist = New-Object 'System.Collections.Generic.List[String]'

function GetFilesListAsArray($user,$pass,$local_target,$ftp_uri,$subfolder){
 # ftp address from where to download the files
 $ftp_urix = $ftp_uri + $subfolder
 $uri=[system.URI] $ftp_urix
 
$ftp=[system.net.ftpwebrequest]::Create($uri)
 
if($user)
 {
 $ftp.Credentials=New-Object System.Net.NetworkCredential($user,$pass)
 }
 #Get a list of files in the current directory.
 #Use ListDirectoryDetails instead if you need date, size and other additional file information.
 $ftp.Method=[system.net.WebRequestMethods+ftp]::ListDirectoryDetails
 $ftp.UsePassive=$true
 
try
 {
 $response=$ftp.GetResponse()
 $strm=$response.GetResponseStream()
 $reader=New-Object System.IO.StreamReader($strm,'UTF-8')
 $list=$reader.ReadToEnd()
 $lines=$list.Split("`n")
 return $lines
 }
 catch{
 $_|fl * -Force
 }
}

#Brings FTP data into an array
$FTParray = GetFilesListAsArray $user1 $pass1 $local_target1 $ftp_uri1 $subfolder1

[regex]$FTPrx="$($info.fileNamePrefix)(.*)(?=.zip)" # Captures file name only. Double quotes needed here to retain variable concatination
[regex]$DateTimeRegex = '(\w+)\s+(\w+)\s+(\w+)\:(\w+)' # Captures Month/Day/Time of Last Modified (upload time)

#If the item in the list contains a search term (from the file name) then keep it. 
foreach ($file in $FTParray){
	 if ($file -match ($info.fileNamePrefix)) { # This will remove directories and leave only files.  
		$theDate = get-date
		#$timeString = $DateTimeRegex.Match($file.ToString()) # This throws an error because it adds a space on days with only one digit (May 14 vs May  3 [2 spaces]) Need to use $detailedTimeString
		$detailedRegex = [regex]::Matches($file,$DateTimeRegex)
		$detailedTimeString = $detailedRegex[0].Groups[1].ToString() + ' ' + $detailedRegex[0].Groups[2].ToString() + ' ' + $detailedRegex[0].Groups[3].ToString() + ':' + $detailedRegex[0].Groups[4].ToString()
		$uploadTime = ([datetime]::parseexact($detailedTimeString, 'MMM d HH:mm', $null)).AddHours(-3) # FTP is EST - Converting to PST. 
		if($theDate -gt $uploadTime.AddMinutes(15)){ #Only add a file to the download list if it was published greater than 15 mins ago.
			$FTPlist.Add($FTPrx.Match($file.ToString())) # Add file name to download list (will compare with AWS)
		}
	 }
}

#$FTPlist

##################################################    Compares Lists   ################################################################################

#Compares the two lists. Keeps only things that are present in FTP and NOT in S3.
$final = $FTPlist | ?{$AWSlist -notcontains $_}

#$final

#################################################    Downloads Delta   ################################################################################

function DownloadFile ($sourceuri,$targetpath,$username,$password){
 # Create a FTPWebRequest object to handle the connection to the ftp server
 $ftprequest = [System.Net.FtpWebRequest]::create($sourceuri)
 
# set the request's network credentials for an authenticated connection
 $ftprequest.Credentials = New-Object System.Net.NetworkCredential($username,$password)
 
$ftprequest.Method = [System.Net.WebRequestMethods+Ftp]::DownloadFile
 $ftprequest.UseBinary = $true
 $ftprequest.KeepAlive = $false
 
# send the ftp request to the server
 $ftpresponse = $ftprequest.GetResponse()
 
# get a download stream from the server response
 $responsestream = $ftpresponse.GetResponseStream()
 
# create the target file on the local system and the download buffer
 try
 {
 $targetfile = New-Object IO.FileStream ($targetpath,[IO.FileMode]::Create)
 "File created: $targetpath"
 [byte[]]$readbuffer = New-Object byte[] 1024
 
# loop through the download stream and send the data to the target file
 do{
 $readlength = $responsestream.Read($readbuffer,0,1024)
 $targetfile.Write($readbuffer,0,$readlength)
 }
 while ($readlength -ne 0)
 
$targetfile.close()
 }
 catch
 {
 $_|fl * -Force
 }
 
}

[regex]$Finalrx='[^_]*$'
foreach ($thing in $final){
	$file_name = $thing.ToString().Trim()+".zip"
	$source = $ftp_uri1 + $subfolder1 + $file_name
	$target = $local_target1 + $file_name
	DownloadFile $source $target $user1 $pass1

	#Extract The file
	Get-ChildItem ($info.localTarget + '*.zip') | % {& "C:\Program Files\7-Zip\7z.exe" "x" $_.fullname ("-o" + $info.localTarget)}

	#Delete the zip file
	Get-ChildItem -Path ($info.localTarget) -Include *.zip -File -Recurse | foreach { $_.Delete()}
	###Remove-Item C:\FTPTest\*.zip

	#Zip to GZip
	Get-ChildItem ($info.localTarget + '*.csv') | % {& "C:\Program Files\7-Zip\7z.exe" a -tgzip ($_.FullName+".gz") $_.FullName}

	#Send the file to S3
	$first = $info.localTarget
	$second = $info.fileNamePrefix
	$third = $Finalrx.Match($thing.ToString())
	$fourth = ".csv.gz"
	$fullpath =  "$first$second$third$fourth"

	#The S3 path is different depending on whether the bucket is partitioned
	if ($info.partitioned -eq $true){
	Write-S3Object -BucketName ($info.S3BucketName + '/dt=' + $third) -File $fullpath
		} Else { 
			Write-S3Object -BucketName ($info.S3BucketName) -File $fullpath
		}
 
	#Delete the csv files
	Get-ChildItem -Path $info.localTarget -Include *.csv -File -Recurse | foreach { $_.Delete()}

	#Delete the gzip files
	Get-ChildItem -Path $info.localTarget-Include *.gz -File -Recurse | foreach { $_.Delete()}

	#Send text message alert
	aws sns publish --message ($info.friendlyName + ': ' + $third + ' file upload complete.') --phone-number "+1425xxxxxxx"

	#Wait to ensure S3 has rec'd/processed file before proceeding
	Start-Sleep -s 10

	if ($info.partitioned -eq $true){
		#Add the new partition to the hive metastore
		Start-ATHQueryExecution -QueryString ("MSCK REPAIR TABLE " + $info.athenaTable) -ClientRequestToken (New-Guid) -QueryExecutionContext_Database "historical" -ResultConfiguration_OutputLocation s3://aws-athena-query-results-spearhead
	}

	#After Equity Interval file downloads, run Monte Carlo script
	if ($info.friendlyname -eq '[Equity Interval]'){
		
		$instance = "i-xxxxxxxxxxxxxx"

		Start-EC2Instance -InstanceId $instance

		While((Get-EC2Instance -InstanceId $instance).Instances[0].State.Name -ne 'running'){
			Write-Verbose "Waiting for instance to start"
			Start-Sleep -s 10 
		}

		While((Get-EC2InstanceStatus -InstanceId $instance).Status.Status.Value -ne 'ok'){
			Write-Verbose "Waiting for instance to initialize"
			Start-Sleep -s 30
			#If needed in the future: Increment here an after 10 mins send SNS saying there's trouble starting instance. 
		}

		#Run Local Python Script (Ensure that the AWS Private IP/hostname is being used)
		cat "C:\MonteCarloStock.py" |  ssh -i "C:\xxx.pem" ubuntu@ip-172-xx-xx-xx.ec2.internal -o StrictHostKeyChecking=no python -

		Stop-EC2Instance -InstanceId $instance

		While((Get-EC2Instance -InstanceId $instance).Instances[0].State.Name -ne 'stopped'){ #Can be changed to 'stopping' when in prod. 
		Write-Verbose "Waiting for instance to stop"
		Start-Sleep -s 10
		}

		#Add the new dt partition
		Start-ATHQueryExecution -QueryString ("MSCK REPAIR TABLE " + "montecarloresults") -ClientRequestToken (New-Guid) -QueryExecutionContext_Database "historical" -ResultConfiguration_OutputLocation s3://aws-athena-query-results-spearhead
		
		#Send text message alert
		aws sns publish --message ($info.friendlyName + ': ' + $third + ' Monte Carlo Simulation complete.') --phone-number "+1425xxxxxxx"
	}
}

}
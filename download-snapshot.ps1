$baseUrl = 'https://download.nine-chronicles.com/partition/'
$latest = "$($baseUrl)latest.json"
$latestState = "$($baseUrl)state_latest.zip"

$epoch = Invoke-WebRequest $latest |
ConvertFrom-Json |
Select BlockEpoch, TxEpoch, PreviousBlockEpoch, PreviousTxEpoch

$myarray = [System.Collections.ArrayList]::new()
[void]$myarray.Add($epoch)

echo "Calculating total amount of snapshots to download."

while($true)
{
	$nextEpochMeta = "$($baseUrl)snapshot-$($epoch.PreviousBlockEpoch)-$($epoch.PreviousTxEpoch).json"
	$epoch = Invoke-WebRequest $nextEpochMeta |
		ConvertFrom-Json |
		Select BlockEpoch, TxEpoch, PreviousBlockEpoch, PreviousTxEpoch
	[void]$myarray.Add($epoch)
	if( 0 -eq $epoch.PreviousBlockEpoch )
	{
		break;
	}
}

echo "Calculate finish. Number of $($myarray.count) snapshot download start."
foreach($currentEpoch in $myarray) {
	$filename = "snapshot-$($currentEpoch.BlockEpoch)-$($currentEpoch.TxEpoch).zip"
	echo "Download $($filename)."
	$nextsnapshot = "$($baseUrl)$($filename)"
	Invoke-WebRequest -Uri $nextsnapshot -Outfile $filename
}

$filename = "state_latest.zip"
echo "Download State."
Invoke-WebRequest -Uri  $latestState -Outfile $filename

echo "Download finish. snapshot extracting start."
$myarray.Reverse()

foreach($currentEpoch in $myarray) {
	$filename = "snapshot-$($currentEpoch.BlockEpoch)-$($currentEpoch.TxEpoch).zip"
	echo "Extract $($filename)."
	Expand-Archive -Path $filename -DestinationPath 9c-main-partition -Force
}

$filename = "state_latest.zip"
echo "Extract State."
Expand-Archive -Path $filename -DestinationPath 9c-main-partition -Force

echo "Extract finish. all snapshot download finish"


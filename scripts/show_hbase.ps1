# Show real HBase records from the Explainable MRI project (P-b7268ef2), run with OpenCode-1.
#   powershell -ExecutionPolicy Bypass -File scripts\show_hbase.ps1
$prefix = "04#PROJECT#P-b7268ef2"
$shell = @"
list
scan 'events', {ROWPREFIXFILTER => '$prefix', COLUMNS => ['e:event_type','e:participant_id','e:occurred_at'], LIMIT => 6}
scan 'tasks', {ROWPREFIXFILTER => '$prefix', COLUMNS => ['task:title','state:status','state:participant'], LIMIT => 5}
"@
Write-Output "=== HBase: tables, then real events and tasks from project P-b7268ef2 ==="
$shell | docker exec -i palinor-hbase hbase shell -n 2>$null

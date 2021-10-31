# CSVLog Plugin For HMQ
This is a bridge implementation for HMQ that allows messages to be logged to a CSV file at runtime.

It can be used for debugging/monitoring purposes, for integration with other systems/platforms, or as an audit trail of messages.

The plugin allows you to define 0, 1, or more filters which determine which messages get bridged. Where no filters are defined the plugin bridges every message. Where one or more filters exist, the plugin applies  the filter/s and only brdiges messages that match the filter spec.

The plugin allows you provide a filename for the output file, and also supports three special filenames {LOG},{STDOUT}, and {NULL}. {LOG} results in messages being bridged to the log, {STDOUT} bridges them to Std out, and {NULL} simply skips and returns without an error.

## Configuration
The configiration settings for CSVLog are defined by the struct csvBridgeConfig.
```
type csvBridgeConfig struct {
	FileName          string   `json:"fileName"`
	LogFileMaxSizeMB  int64    `json:"logFileMaxSizeMB"`
	LogFileMaxFiles   int64    `json:"logFileMaxFiles"`
	WriteIntervalSecs int64    `json:"writeIntervalSecs"`
	CommandTopic      string   `json:"commandTopic"`
	Filters           []string `json:"filters"`
}
```
| Setting | Description |
| ----------- | ----------- |
| FileName | A complete filename for the output file, or {LOG} to send bridged messages to the log, {STDOUT} to send bridged messages to STDOUT, or {NULL} to not bridge anything at all |
| LogFileMaxSizeMB | The size in megabytes at which the log file is rotated |
| LogFileMaxFiles | The maximum number of rotated logfiles to retain before they're deleted |
| WriteIntervalSecs | The delay before flushing any pending writes to the file |
| CommandTopic | The name of a topic to which commands relating to CSVLog will be sent eg "bridge/CSVLOG/command" |
| Filters | An array of filter specifications which are used to determine which messages are bridged, if there are no filters specified the filter is assumed to be "#" which bridges everything. Filters are specified the same way that topic acls are described|

## Filters

Filters use the same syntax as for ACL permissions.

So a filter can name a specific topic..

"animals/cats" will bridge messages sent to the "animals/cats" topic. 

A filter can use the + or # wildcards so

"animals/cats/+" will bridge messages sent to "animals/cats/breeds", "animals/cats/colours" but not "animals/cats/breeds/longhair"

"animals/cats/#" will bridge messages sent to "animals/cats/breeds", "animals/cats/colours", "animals/cats/breeds/longhair", etc

## Commands
Currently two commands can be sent to the CSVLog bridge:

ROTATEFILE - Triggers an immediate rotation of the log file

REALOADCONFIG - Triggers a reload of the CSVLog config file

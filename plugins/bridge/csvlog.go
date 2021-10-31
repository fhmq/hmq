package bridge

/*
Copyright (c) 2021, Gary Barnett @thinkovation. Released under the Apache 2 License

CSVLog is a bridge plugin for HMQ that implements CSV logging of messages. See CSVLog.md for more information

*/

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type csvBridgeConfig struct {
	FileName          string   `json:"fileName"`
	LogFileMaxSizeMB  int64    `json:"logFileMaxSizeMB"`
	LogFileMaxFiles   int64    `json:"logFileMaxFiles"`
	WriteIntervalSecs int64    `json:"writeIntervalSecs"`
	CommandTopic      string   `json:"commandTopic"`
	Filters           []string `json:"filters"`
}

type csvLog struct {
	config  csvBridgeConfig
	buffer  []string
	msgchan chan (*Elements)

	sync.RWMutex
}

// rotateLog performs a log rotation - copying the current logfile to the base file name plus a timestamp
func (c *csvLog) rotateLog(withPrune bool) error {
	c.Lock()
	filename := c.config.FileName
	c.Unlock()

	basename := strings.TrimSuffix(filename, filepath.Ext(filename))
	newpath := basename + time.Now().Format("-20060102T150405") + filepath.Ext(filename)
	renameError := os.Rename(filename, newpath)
	if renameError != nil {
		return renameError
	}
	outfile, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	outfile.Close()
	// Whenever we rotate a logfile we prune
	if withPrune {
		c.logFilePrune()

	}
	return nil

}

// writeToLog takes an array of elements and writes them to the logfile (or to log or stdout) spefified in
// the configuration
func (c *csvLog) writeToLog(els []Elements) error {

	c.RLock()
	fname := c.config.FileName
	c.RUnlock()
	if fname == "" {
		fname = "CSVLOG.CSV"
	}

	if fname == "{LOG}" {
		for _, value := range els {
			t := time.Unix(value.Timestamp, 0)
			log.Info(t.Format("2006-01-02T15:04:05") + " " + value.ClientID + " " + value.Username + " " + value.Action + " " + value.Topic + " " + value.Payload)
		}
		return nil
	}
	if fname == "{STDOUT}" {
		for _, value := range els {
			t := time.Unix(value.Timestamp, 0)
			fmt.Println(t.Format("2006-01-02T15:04:05") + " " + value.ClientID + " " + value.Username + " " + value.Action + " " + value.Topic + " " + value.Payload)
		}
		return nil
	}

	var mbsize int64
	fileStat, fileStatErr := os.Stat(fname)
	if fileStatErr != nil {
		log.Warn("Could not get CSVLog info. Received Err " + fileStatErr.Error())
		mbsize = 0
	} else {
		mbsize = fileStat.Size() / 1024 / 1024
	}
	if mbsize > c.config.LogFileMaxSizeMB && c.config.LogFileMaxSizeMB != 0 {
		rotateErr := c.rotateLog(true)
		if rotateErr != nil {
			log.Warn("Unable to rotate outputfile")
		}
	}
	outfile, outfileOpenError := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer outfile.Close()
	if outfileOpenError != nil {
		log.Warn("Could not open CSV Log file to write")
		return errors.New("Could not write to CSV Log File")
	}

	writer := csv.NewWriter(outfile)
	defer writer.Flush()

	for _, value := range els {
		t := time.Unix(value.Timestamp, 0)
		var outrow = []string{t.Format("2006-01-02T15:04:05"), value.ClientID, value.Username, value.Action, value.Topic, value.Payload}
		writeOutRowError := writer.Write(outrow)
		if writeOutRowError != nil {
			log.Warn("Could not write msg to CSV Log")
		}
	}
	return nil

}

// Worker should be invoked as a goroutine - It listens on the csvlog message channel for incoming messages
// for performance we batch messages into an outqueue and write them in bulk when a timer expires
func (c *csvLog) Worker() {
	log.Info("Running CSVLog worker")
	run := true
	var outqueue []Elements

	for run == true {
		c.RLock()
		waitInterval := c.config.WriteIntervalSecs
		c.RUnlock()

		timer := time.NewTimer(time.Second * time.Duration(waitInterval))

		select {

		case p := <-c.msgchan:
			c.RLock()

			oktopublish := false

			// Check to see if any filters are defined. If there are none we assume we're logging everything
			if len(c.config.Filters) != 0 {
				// We pick up a Read lock here to parse the c.config.Filters string array
				// as it's a read lock, and write locks will be rare
				// it feels as if this will be fine.
				// If there is contention, it _might_ make sense to quickly lock c, get
				// the filters and release the lock, then process the filters with no locks
				// but I think it's unlikely

				for _, filt := range c.config.Filters {
					if topicMatch(p.Topic, filt) {
						oktopublish = true
						break

					}

				}

			} else {
				oktopublish = true
			}
			if oktopublish {
				var el Elements
				el.Action = p.Action
				el.ClientID = p.ClientID
				el.Payload = p.Payload
				el.Size = p.Size
				el.Timestamp = p.Timestamp
				el.Topic = p.Topic
				el.Username = p.Username
				outqueue = append(outqueue, el)
			}
			c.RUnlock()
			break
		case <-timer.C:
			if len(outqueue) > 0 {
				writeResult := c.writeToLog(outqueue)
				if writeResult != nil {
					log.Warn("Trouble writing to CSV Log")
				}
				outqueue = nil

			}
			break
		}
		if run != true {
			log.Info("Closing CSV Bridge worker")
			break
		}
	}
}

// LoadCSVLogConfig loads the configuration file - it currently looks in
// "./plugins/csvlog/csvlogconfig.json" (following the example of the default location of the kafka plugin config file)
// if it doesn't find it there it looks in two further places - the current directory and
// an "assets" folder under the current directory (This is for compatibility with a couple of deployed)
// implementations.
func LoadCSVLogConfig() csvBridgeConfig {
	// Check to see if the CSVLOGCONFFILE environment variable is set and if so
	// check that it does actually point to a file
	csvLogConfigFile := os.Getenv("CSVLOGCONFFILE")
	if csvLogConfigFile != "" {
		if _, err := os.Stat(csvLogConfigFile); os.IsNotExist(err) {
			csvLogConfigFile = ""
		}
	}
	// If csvLogConfigFile is blank look in the plugins directory,
	// then the current directory for the csvLogConfigFile. If it's still not found we use a default config
	// If the file does not exist, we use default parameters
	if csvLogConfigFile == "" {
		csvLogConfigFile = "./plugins/csvlog/csvlogconfig.json"
	}
	if _, err := os.Stat(csvLogConfigFile); os.IsNotExist(err) {

		if _, err := os.Stat("csvlogconfig.json"); os.IsNotExist(err) {
			csvLogConfigFile = ""
		} else {
			csvLogConfigFile = "csvlogconfig.json"
		}
	}

	var configUnmarshalErr error
	var config csvBridgeConfig
	if csvLogConfigFile != "" {
		log.Info("Trying to load config file from " + csvLogConfigFile)
		content, err := ioutil.ReadFile(csvLogConfigFile)
		if err != nil {
			log.Info("Read config file error: ", zap.Error(err))
		}
		configUnmarshalErr = json.Unmarshal(content, &config)
	}

	if configUnmarshalErr != nil || config.FileName == "" {
		log.Warn("Unable to load csvlog config file, so using default settings")
		config.FileName = "/var/log/csvlog.log"
		config.CommandTopic = "CSVLOG/command"
		config.WriteIntervalSecs = 10
		config.LogFileMaxSizeMB = 1
		config.LogFileMaxFiles = 4

	}
	return config

}

// InitCSVLog initialises a CSVLOG plugin
// It does this by loading a config file if one can be found. The default filename follows the same
// convention as the kafka plugin - ie it's in "./plugins/csvlog/csvlogconfig.json" but an
// environment var - CSVLOGCONFFILE - can be set to provide a different location.
//
// Once the config is set the worker is started
func InitCSVLog() *csvLog {
	log.Info("Trying to init CSVLOG")

	c := &csvLog{config: LoadCSVLogConfig()}
	c.msgchan = make(chan *Elements, 200)
	//Start the csvlog worker
	go c.Worker()
	return c

}

// topicMatch accepts a topic name and a filter string, it then evaluates the
// topic against the filter string and returns true if there is a match.
//
// The CSV bridge can be configured with 0, 1 or more filters - Where there are no
// filters specified, every message will be re-published. Where there are filters, any message
// that passes any of the filter tests will be re-published.
func topicMatch(topic string, filter string) bool {
	if topic == filter || filter == "#" {
		return true
	}
	topicComponents := strings.Split(topic, "/")
	filterComponents := strings.Split(filter, "/")
	currentpos := 0
	filterComponentsLength := len(filterComponents)
	currentFilterComponent := ""
	if filterComponentsLength > 0 {
		currentFilterComponent = filterComponents[currentpos]
	}
	for _, topicVal := range topicComponents {
		if currentFilterComponent == "" {
			return false
		}
		if currentFilterComponent == "#" {
			return true
		}
		if currentFilterComponent != "+" && currentFilterComponent != topicVal {
			return false
		}
		currentpos++
		if filterComponentsLength > currentpos {
			currentFilterComponent = filterComponents[currentpos]
		} else {
			currentFilterComponent = ""
		}
	}
	return true
}

// logFilePrune checks the number of rotated logfiles and prunes them
func (c *csvLog) logFilePrune() error {

	// List the rotated files
	c.RLock()
	filename := c.config.FileName
	maxfiles := c.config.LogFileMaxFiles
	c.RUnlock()
	if maxfiles == 0 {
		return nil
	}

	fileExt := filepath.Ext(filename)
	fileDir := filepath.Dir(filename)
	baseFileName := strings.TrimSuffix(filepath.Base(filename), fileExt)

	files, err := ioutil.ReadDir(fileDir)
	if err != nil {
		return err
	}

	var foundFiles []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), baseFileName+"-") {

			foundFiles = append(foundFiles, file.Name())

		}
	}
	if len(foundFiles) >= int(maxfiles) {
		fmt.Println("Found ", len(foundFiles), " files")
		sort.Strings(foundFiles)
		for i := 0; i < len(foundFiles)-int(maxfiles); i++ {
			fileDeleteError := os.Remove(fileDir + "//" + foundFiles[i])
			log.Info("Pruning logfile " + fileDir + "//" + foundFiles[i])
			if fileDeleteError != nil {
				log.Warn("Could not delete file " + fileDir + "//" + foundFiles[i])

			}

		}

	}

	return nil

}

// Publish implements the bridge interface - it accepts an Element then checks to see if that element is a
// message published to the admin topic for the plugin
//
func (c *csvLog) Publish(e *Elements) error {
	// A short-lived lock on c allows us to
	// get the Command topic then release the lock
	// This then allows us to process the command - which may
	// take its a write lock on c (to update values) and then
	// return here where we'll pick up a
	// read lock to iterate over the c.config.filters
	// We're trying to minimise the time spent in this function
	// and to limit the overall time spent in any write locks.
	c.RLock()
	//CSVLOG allows you to configure a CommandTopic which is a topic to which commands affecting the behaviour of CSVLog can be sent
	//The simplest would be a message with a payload of "RELOAD" which will reload the configuration allowing configuration changes to be
	//made at runtime without restarting the broker
	CommandTopic := c.config.CommandTopic
	OutFile := c.config.FileName
	c.RUnlock()
	// If the outfile is set to "{NULL}" we don't do anything with the message - we just return nil
	// This feature is here to allow CSVLOG to be enabled/disabled at runtime
	if OutFile == "{NULL}" {
		return nil
	}

	if e.Topic == CommandTopic {

		log.Info("CSVLOG Command Received")

		// Process Command
		// These are going to be rare ocurrences, so in this implementation
		// we will process the command here - but if we _really_ want to
		// squeeze delays out, we could have a worker sitting on a
		// command channel processing any commands.
		if e.Payload == "RELOADCONFIG" {
			newConfig := LoadCSVLogConfig()
			c.Lock()
			c.config = newConfig
			c.Unlock()

		}
		if e.Payload == "ROTATEFILE" {

			c.rotateLog(true)

		}
		if e.Payload == "ROTATEFILENOPRUNE" {

			c.rotateLog(false)

		}
		// We could return without doing anything more here, but
		// for now we move ahead with the filter processing on the
		// basis that unless we either filter for "all" (with #) or
		// filter for the CommandTopic, they won't be logged - but we
		// may have a reason for wanting to track commands too
	}
	// Push the message into the channel and return
	// the channel is buffered and is read by a goroutine so this should block for the shortest possible time
	c.msgchan <- e
	return nil
}

package broker

import (
	"fmt"
)

// StartPipeSocketListening We use the open source npipe library
// to jump over pipe communication in mac
func (b *Broker) StartPipeSocketListening(pipeName string, usePipe bool) {
	fmt.Println("macos system")
}

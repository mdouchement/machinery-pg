package machinerypg

import (
	"github.com/RichardKnop/machinery/v1/logger"
)

var log logger.Interface

func init() {
	log = logger.Get()
}

func SetLogger(l logger.Interface) {
	log = l
}

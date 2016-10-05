package machinerypg

import (
	"github.com/RichardKnop/machinery/v1/logger"
)

var logg logger.Interface

func init() {
	logg = logger.Get()
}

func SetLogger(l logger.Interface) {
	logg = l
}

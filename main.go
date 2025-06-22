package main

import (
	"github.com/JaySon-Huang/tiflash-ctl/cmd"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cmd.Execute()
}

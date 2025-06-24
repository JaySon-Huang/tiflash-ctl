package main

import (
	"github.com/JaySon-Huang/tiflash-ctl/cmd"
	"github.com/JaySon-Huang/tiflash-ctl/pkg/logutil"
)

func main() {
	defer logutil.BgLogger().Sync()
	cmd.Execute()
}

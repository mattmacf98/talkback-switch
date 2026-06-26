package main

import (
	"talkbackswitch"
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/resource"
	sw "go.viam.com/rdk/components/switch"
)

func main() {
	// ModularMain can take multiple APIModel arguments, if your module implements multiple models.
	module.ModularMain(resource.APIModel{ sw.API, talkbackswitch.TalkbackSwitch})
}

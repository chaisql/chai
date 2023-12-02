package commands

import (
	"fmt"
	"runtime/debug"

	"github.com/urfave/cli/v2"
)

// NewVersionCommand returns a cli.Command for "chai version".
func NewVersionCommand() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "Shows Chai and Chai CLI version",
		Action: func(c *cli.Context) error {
			var cliVersion, chaiVersion string
			info, ok := debug.ReadBuildInfo()

			if !ok {
				fmt.Println(`version not available in GOPATH mode; use "go get" with Go modules enabled`)
				return nil
			}

			cliVersion = info.Main.Version
			for _, mod := range info.Deps {
				if mod.Path != "github.com/chaisql/chai" {
					continue
				}
				// if a replace directive is set, Chai is in development mode
				if mod.Replace != nil {
					chaiVersion = "(devel)"
					break
				}
				chaiVersion = mod.Version
				break
			}
			fmt.Printf("Chai %v\nChai CLI %v\n", chaiVersion, cliVersion)
			return nil
		},
	}
}

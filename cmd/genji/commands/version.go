package commands

import (
	"fmt"
	"runtime/debug"

	"github.com/urfave/cli/v2"
)

// NewVersionCommand returns a cli.Command for "genji version".
func NewVersionCommand() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "Shows Genji and Genji CLI version",
		Action: func(c *cli.Context) error {
			var cliVersion, genjiVersion string
			info, ok := debug.ReadBuildInfo()

			if !ok {
				fmt.Println(`version not available in GOPATH mode; use "go get" with Go modules enabled`)
				return nil
			}

			cliVersion = info.Main.Version
			for _, mod := range info.Deps {
				if mod.Path != "github.com/genjidb/genji" {
					continue
				}
				// if a replace directive is set, Genji is in development mode
				if mod.Replace != nil {
					genjiVersion = "(devel)"
					break
				}
				genjiVersion = mod.Version
				break
			}
			fmt.Printf("Genji %v\nGenji CLI %v\n", genjiVersion, cliVersion)
			return nil
		},
	}
}

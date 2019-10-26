package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "Genji"
	app.Usage = "Toolkit for the Genji database"
	app.Version = "v0.3.0"
	app.EnableBashCompletion = true
	app.Commands = []cli.Command{
		{
			Name:    "generate",
			Aliases: []string{"gen"},
			Usage:   "scan a structure and generate methods implementing various Genji interfaces",
			Flags: []cli.Flag{
				cli.StringSliceFlag{
					Name:     "f",
					Required: true,
					Usage:    "paths of the files to parse",
				},
				cli.StringSliceFlag{
					Name:     "s",
					Required: true,
					Usage:    "names of the source structures",
				},
				cli.StringFlag{
					Name:  "output, o",
					Usage: "name of the generated file",
				},
			},
			Action: func(c *cli.Context) error {
				files := c.StringSlice("f")
				structs := c.StringSlice("s")
				if len(files) == 0 {
					return cli.NewExitError("missing files", 2)
				}

				if len(structs) == 0 {
					return cli.NewExitError("missing structs", 2)
				}

				return generate(c.StringSlice("f"), c.StringSlice("s"), c.String("o"))
			},
		},
	}

	app.Action = func(c *cli.Context) error {
		return runGenjiClient()
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(2)
}

func exitRecordUsage() {
	flag.Usage()
	os.Exit(2)
}

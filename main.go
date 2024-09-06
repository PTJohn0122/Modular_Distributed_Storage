package main

import (
	"bufio"
	"context"
	"fmt"
	"modist/client"
	"modist/orchestrator"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	cmd = &cobra.Command{
		Use:   "modist",
		Short: "A modular, distributed key-value store",
		Run:   run,
	}
	configPathArg string
)

func main() {
	cmd.Flags().StringVarP(&configPathArg, "config", "c", "config/base.yaml", "Path to the configuration file")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	rawCfg, err := os.ReadFile(configPathArg)
	if err != nil {
		fmt.Printf("Unable to open the config file: %v\n", err)
		os.Exit(1)
	}

	cfg := orchestrator.Config{}
	err = yaml.Unmarshal(rawCfg, &cfg)
	if err != nil {
		fmt.Printf("Unable to parse the config file at location %q: %v\n", configPathArg, err)
		os.Exit(1)
	}

	h, err := orchestrator.New(ctx, cfg)
	if err != nil {
		fmt.Printf("Unable to create the orchestrator: %v\n", err)
		os.Exit(1)
	}

	// Setup clients for every partitioner and allow the user to switch between them.
	clients := map[string]*client.Client{}
	for _, p := range cfg.Partitioning {
		c, err := h.Client(p.Name)
		if err != nil {
			fmt.Printf("Unable to create the client for partitioner %s: %v\n", p.Name, err)
			os.Exit(1)
		}
		clients[p.Name] = c
	}
	curClient := clients[cfg.Partitioning[0].Name]

	time.Sleep(500 * time.Millisecond)
	fmt.Println("\n==================\nWelcome to Modist!\n==================")
	if len(cfg.Partitioning) > 1 {
		fmt.Println("Using partitioner:", cfg.Partitioning[0].Name)
	}

	// Start a REPL
	reader := bufio.NewReader(os.Stdin)

	for {
		// Print a prompt to the user
		fmt.Print(">>> ")

		// Read a line of input from the user
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Split the input into a slice of words
		words := strings.Fields(input)

		// Check the first word to determine the operation to perform
		if len(words) == 0 {
			continue
		}

		switch strings.ToLower(words[0]) {
		case "get":
			// Get the value for the specified key
			if len(words) != 2 {
				fmt.Println("Invalid number of arguments for get operation: get <key>")
				continue
			}
			key := words[1]

			value, err := curClient.Get(ctx, key, client.WithConsistency(client.ConsistencyNone))
			if err != nil {
				fmt.Println("Unable to fetch the value for the key:", err)
				continue
			}

			fmt.Println(value)

		case "put":
			// Set the value for the specified key
			if len(words) != 3 {
				fmt.Println("Invalid number of arguments for put operation: put <key> <value>")
				continue
			}

			key := words[1]
			value := words[2]

			err := curClient.Put(ctx, key, value)
			if err != nil {
				fmt.Println("Unable to set the value for the key:", err)
				continue
			}

			fmt.Println("Successfully set value for key")

		case "use":
			// Switch to a different partitioner
			if len(words) != 2 {
				fmt.Println("Invalid number of arguments for use operation: use <partitioner_name>")
				continue
			}

			partitionerName := words[1]
			if c, ok := clients[partitionerName]; ok {
				curClient = c
			} else {
				fmt.Println("No partitioner with the specified name exists")
				continue
			}

		default:
			fmt.Println(`Invalid operation.
Usage:
	get <key>
	put <key> <value>
	use <partitioner_name>`)
		}
	}
}

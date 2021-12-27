package urfavtract

import (
	"strings"

	tract "github.com/23caterpie/Tract"

	"github.com/urfave/cli/v2"
)

func NewWorkerTractConfig(name string) WorkerTractConfig {
	return WorkerTractConfig{
		name: name,
		Size: 1,
	}
}

type WorkerTractConfig struct {
	// name is used as the name of the tract and for envirnment variable configuration.
	name string
	// Size is the configurable size of the tract.
	Size int
}

func (c WorkerTractConfig) Flags() []cli.Flag {
	tractSizeFlagName := "tract-" + c.name + "-size"
	tractSizeFlagEnvar := strings.ToUpper(strings.Replace(tractSizeFlagName, "-", "_", -1))
	return []cli.Flag{
		&cli.IntFlag{
			Name:        tractSizeFlagName,
			EnvVars:     []string{tractSizeFlagEnvar},
			Destination: &c.Size,
			Value:       c.Size,
		},
	}
}

func NewWorkerTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	workerFactory tract.WorkerFactory[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerTract(
		config.name,
		config.Size,
		workerFactory,
	)
}

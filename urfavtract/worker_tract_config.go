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

func (c *WorkerTractConfig) Flags() []cli.Flag {
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
	worker tract.Worker[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerTract(config.name, config.Size, worker)
}

func NewWorkerFactoryTract[InputType, OutputType tract.Request, WorkerType tract.Worker[InputType, OutputType]](
	config WorkerTractConfig,
	workerFactory tract.WorkerFactory[InputType, OutputType, WorkerType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerFactoryTract(config.name, config.Size, workerFactory)
}

func NewWorkerFuncTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	f tract.WorkerFunc[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerFuncTract(config.name, config.Size, f)
}

func NewBasicWorkerFuncTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	f tract.BasicWorkerFunc[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewBasicWorkerFuncTract(config.name, config.Size, f)
}

func NewErrorWorkerFuncTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	f tract.ErrorWorkerFunc[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewErrorWorkerFuncTract(config.name, config.Size, f)
}

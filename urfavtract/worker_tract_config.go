package urfavtract

import (
	"strings"

	tract "github.com/23caterpie/Tract"

	"github.com/urfave/cli/v2"
)

// NewWorkerTractConfig defines a new WorkerTractConfig for a tract named @name.
// usage:
//     myTractConfig := urfavtract.NewWorkerTractConfig("my-amazing-worker-tract")
//     ...
//     myAppsFlags = append(myAppsFlags, myTractConfig.Flags()...)
//     ...
//     myTract = urfavtract.NewWorkerTract(myTractConfig, myWorker)
func NewWorkerTractConfig(name string) WorkerTractConfig {
	return WorkerTractConfig{
		name: name,
		Size: 1,
	}
}

// WorkerTractConfig defines a work tract you will use by it's name, and contains configurable fields for it.
type WorkerTractConfig struct {
	// name is used as the name of the tract and for envirnment variable configuration.
	name string
	// Size is the configurable size of the tract.
	Size int
}

// Flags returns all the cli flags needed to configure WorkerTractConfig.
// These flags should be provided to the App or Command's Flag list.
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

// Name returns the name of the tract this config is for.
func (c WorkerTractConfig) Name() string {
	return c.name
}

// NewWorkerTract calls tract.NewWorkerTract with the configured fields.
func NewWorkerTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	worker tract.Worker[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerTract(config.name, config.Size, worker)
}

// NewWorkerFactoryTract calls tract.NewWorkerFactoryTract with the configured fields.
func NewWorkerFactoryTract[InputType, OutputType tract.Request, WorkerType tract.Worker[InputType, OutputType]](
	config WorkerTractConfig,
	workerFactory tract.WorkerFactory[InputType, OutputType, WorkerType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerFactoryTract(config.name, config.Size, workerFactory)
}

// NewWorkerFuncTract calls tract.NewWorkerFuncTract with the configured fields.
func NewWorkerFuncTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	f tract.WorkerFunc[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewWorkerFuncTract(config.name, config.Size, f)
}

// NewBasicWorkerFuncTract calls tract.NewBasicWorkerFuncTract with the configured fields.
func NewBasicWorkerFuncTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	f tract.BasicWorkerFunc[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewBasicWorkerFuncTract(config.name, config.Size, f)
}

// NewErrorWorkerFuncTract calls tract.NewErrorWorkerFuncTract with the configured fields.
func NewErrorWorkerFuncTract[InputType, OutputType tract.Request](
	config WorkerTractConfig,
	f tract.ErrorWorkerFunc[InputType, OutputType],
) tract.Tract[InputType, OutputType] {
	return tract.NewErrorWorkerFuncTract(config.name, config.Size, f)
}

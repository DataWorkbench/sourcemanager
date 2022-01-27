package handler

import (
	"github.com/DataWorkbench/common/constants"
	"github.com/DataWorkbench/common/utils/idgenerator"
	"github.com/DataWorkbench/gproto/pkg/enginepb"
	"gorm.io/gorm"
)

// global options in this package.
var (
	dbConn *gorm.DB

	datasourceIdGenerator *idgenerator.IDGenerator

	EngineClient enginepb.EngineClient
)

type Option func()

func WithDBConn(conn *gorm.DB) Option {
	return func() {
		dbConn = conn
	}
}

func initIdGenerator() {
	datasourceIdGenerator = idgenerator.New(constants.IdPrefixDatasource)
}

func Init(opts ...Option) {
	for _, opt := range opts {
		opt()
	}
	initIdGenerator()
}

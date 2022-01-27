package datasource

import (
	"errors"

	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ListDataSourceConnections used to list data source by specified space_id
func ListDataSourceConnections(tx *gorm.DB, input *request.ListDataSourceConnections) (output *response.ListDataSourceConnections, err error) {
	order := input.SortBy
	if order == "" {
		order = "source_id"
	}
	if input.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}

	// Build where exprs.
	exprs := []clause.Expression{
		clause.Eq{
			Column: "source_id",
			Value:  input.SourceId,
		},
		clause.Neq{
			Column: "status",
			Value:  model.DataSource_Deleted.Number(),
		},
	}

	var infos []*model.DataSourceConnection
	var total int64

	err = tx.Table(tableNameDataSourceConnection).Select("*").Clauses(clause.Where{Exprs: exprs}).
		Limit(int(input.Limit)).Offset(int(input.Offset)).Order(order).
		Scan(&infos).Error
	if err != nil {
		return
	}

	// Length of result(infos) less than the limit means no more records with the query conditions.
	if input.Offset == 0 && len(infos) < int(input.Limit) {
		total = int64(len(infos))
	} else {
		err = tx.Table(tableNameDataSourceConnection).Select("count(id)").Clauses(clause.Where{Exprs: exprs}).Count(&total).Error
		if err != nil {
			return
		}
	}

	output = &response.ListDataSourceConnections{
		Infos:   infos,
		Total:   total,
		HasMore: len(infos) >= int(input.Limit),
	}
	return
}

func CreateConnection(tx *gorm.DB, info *model.DataSourceConnection) (err error) {
	if err = tx.Table(tableNameDataSourceConnection).Create(info).Error; err != nil {
		return
	}
	return
}

// DescribeLastConnection query the data source info by id.
func DescribeLastConnection(tx *gorm.DB, sourceId string) (info *model.DataSourceConnection, err error) {
	info = new(model.DataSourceConnection)

	err = tx.Table(tableNameDataSourceConnection).
		Where("source_id = ? AND status != ?", sourceId, model.Workspace_Deleted).
		Take(info).Order("created DESC").
		Error
	if err != nil {
		info = nil
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = nil
		}
		return
	}
	return
}

func DeleteConnectionBySourceIds(tx *gorm.DB, sourceIds []string) (err error) {
	if len(sourceIds) == 0 {
		return
	}

	status := model.DataSourceConnection_Deleted
	eqExpr := make([]clause.Expression, len(sourceIds))
	for i := 0; i < len(sourceIds); i++ {
		eqExpr[i] = clause.Eq{Column: "source_id", Value: sourceIds[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}

	err = tx.Table(tableNameDataSourceConnection).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: status.Number()},
			expr,
		},
	}).Updates(map[string]interface{}{"status": status.Number()}).Error
	if err != nil {
		return
	}
	return
}

func DeleteConnectionBySpaceIds(tx *gorm.DB, spaceIds []string) (err error) {
	if len(spaceIds) == 0 {
		return
	}

	status := model.DataSourceConnection_Deleted
	eqExpr := make([]clause.Expression, len(spaceIds))
	for i := 0; i < len(spaceIds); i++ {
		eqExpr[i] = clause.Eq{Column: "space_id", Value: spaceIds[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}

	err = tx.Table(tableNameDataSourceConnection).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: status.Number()},
			expr,
		},
	}).Updates(map[string]interface{}{"status": status.Number()}).Error
	if err != nil {
		return
	}
	return
}

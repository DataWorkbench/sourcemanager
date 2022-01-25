package datasource

import (
	"errors"
	"time"

	"github.com/DataWorkbench/common/qerror"
	"github.com/DataWorkbench/gproto/pkg/model"
	"github.com/DataWorkbench/gproto/pkg/request"
	"github.com/DataWorkbench/gproto/pkg/response"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ListDataSources used to list data source by specified space_id
func ListDataSources(tx *gorm.DB, input *request.ListDataSources) (output *response.ListDataSources, err error) {
	order := input.SortBy
	if order == "" {
		order = "id"
	}
	if input.Reverse {
		order += " DESC"
	} else {
		order += " ASC"
	}

	// Build where exprs.
	exprs := []clause.Expression{
		clause.Eq{
			Column: "space_id",
			Value:  input.SpaceId,
		},
		clause.Neq{
			Column: "status",
			Value:  model.DataSource_Deleted.Number(),
		},
	}
	if len(input.Search) > 0 {
		// CONCAT(`id`, `name`)
		exprs = append(exprs, clause.Like{
			Column: "name",
			Value:  "%" + input.Search + "%",
		})
	}
	if input.Name != "" && len(input.Search) == 0 {
		exprs = append(exprs, clause.Eq{
			Column: "name",
			Value:  input.Name,
		})
	}

	var infos []*model.DataSource
	var total int64

	err = tx.Table(tableNameDataSource).Select("*").Clauses(clause.Where{Exprs: exprs}).
		Limit(int(input.Limit)).Offset(int(input.Offset)).Order(order).
		Scan(&infos).Error
	if err != nil {
		return
	}

	// Length of result(infos) less than the limit means no more records with the query conditions.
	if input.Offset == 0 && len(infos) < int(input.Limit) {
		total = int64(len(infos))
	} else {
		err = tx.Table(tableNameDataSource).Select("count(id)").Clauses(clause.Where{Exprs: exprs}).Count(&total).Error
		if err != nil {
			return
		}
	}

	output = &response.ListDataSources{
		Infos:   infos,
		Total:   total,
		HasMore: len(infos) >= int(input.Limit),
	}
	return
}

// CreateDataSource creates a new data source.
func CreateDataSource(tx *gorm.DB, info *model.DataSource) (err error) {
	// check exists
	var x string
	err = tx.Table(tableNameDataSource).Select("id").
		Where("space_id = ? AND name = ? AND status != ?", info.SpaceId, info.Name, model.DataSource_Deleted).
		Take(&x).Error
	if err == nil {
		err = qerror.ResourceAlreadyExists
		return
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = nil
	}
	if err != nil {
		return
	}

	if err = tx.Table(tableNameDataSource).Create(info).Error; err != nil {
		return
	}
	return
}

// UpdateDataSource update the data source info.
func UpdateDataSource(tx *gorm.DB, info *model.DataSource) (err error) {
	// check exists
	var x string
	err = tx.Table(tableNameDataSource).Select("id").
		Where("id = ? AND name = ? AND status != ?", info.Id, info.Name, model.DataSource_Deleted).
		Take(&x).Error
	if err == nil {
		err = qerror.ResourceAlreadyExists
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = nil
	}
	if err != nil {
		return
	}
	err = tx.Table(tableNameDataSource).Where("id = ?", info.Id).Updates(info).Error
	return
}

// UpdateDataSourceStatus do update the data source status.
func UpdateDataSourceStatus(tx *gorm.DB, sourceIds []string, status model.DataSource_Status) (err error) {
	if len(sourceIds) == 0 {
		return
	}
	eqExpr := make([]clause.Expression, len(sourceIds))
	for i := 0; i < len(sourceIds); i++ {
		eqExpr[i] = clause.Eq{Column: "id", Value: sourceIds[i]}
	}
	var expr clause.Expression
	if len(eqExpr) == 1 {
		expr = eqExpr[0]
	} else {
		expr = clause.Or(eqExpr...)
	}

	err = tx.Table(tableNameDataSource).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: status.Number()},
			expr,
		},
	}).Updates(map[string]interface{}{"status": status.Number(), "updated": time.Now().Unix()}).Error
	if err != nil {
		return
	}
	return
}

// DescribeDataSource query the data source info by id.
func DescribeDataSource(tx *gorm.DB, sourceId string) (info *model.DataSource, err error) {
	info = new(model.DataSource)
	err = tx.Table(tableNameDataSource).Where("id = ? AND status != ?", sourceId, model.Workspace_Deleted).Take(info).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = qerror.ResourceNotExists
		}
		return
	}
	return
}

// DeleteDataSourceBySourceIds do batch deletes data source.
func DeleteDataSourceBySourceIds(tx *gorm.DB, sourceIds []string) (err error) {
	return UpdateDataSourceStatus(tx, sourceIds, model.DataSource_Deleted)
}

// DeleteDataSourceBySpaceIds delete all data source of the specified workspaces.
func DeleteDataSourceBySpaceIds(tx *gorm.DB, spaceIds []string) (err error) {
	if len(spaceIds) == 0 {
		return
	}
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

	err = tx.Table(tableNameDataSource).Clauses(clause.Where{
		Exprs: []clause.Expression{
			clause.Neq{Column: "status", Value: model.DataSource_Deleted},
			expr,
		},
	}).Updates(map[string]interface{}{"status": model.DataSource_Deleted, "updated": time.Now().Unix()}).Error
	if err != nil {
		return
	}
	return
}

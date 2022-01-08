package db

import (
	"fmt"

	_ "github.com/jinzhu/gorm/dialects/mssql"

	"github.com/jinzhu/gorm"
)

type config interface {
	MysqlDSN() string
	SqliteDSN() string
	SqlServerDSN() string
}

type PageWhereOrder struct {
	Order string
	Where string
	Value []interface{}
}

type DBHandler struct {
	DB *gorm.DB
}

func InitDB(dbType string, cfg config) (dbHandler *DBHandler, err error) {

	dbHandler = &DBHandler{}

	switch dbType {
	case "mysql":

		dbHandler.DB, err = gorm.Open(dbType, cfg.MysqlDSN())
		if err != nil {
			return
		}
	case "sqlite3":
		dbHandler.DB, err = gorm.Open(dbType, cfg.SqliteDSN())
		if err != nil {
			return
		}
	case "mssql":
		dbHandler.DB, err = gorm.Open("mssql", cfg.SqlServerDSN())
		if err != nil {
			return nil, err
		}
	}

	dbHandler.DB.LogMode(true)
	return
}

func (handler *DBHandler) Create(value interface{}) error {

	return handler.DB.Create(value).Error
}

// Save
func (handler *DBHandler) Save(value interface{}) error {
	return handler.DB.Save(value).Error
}

// Updates
func (handler *DBHandler) Updates(table interface{}, where interface{}, value interface{}) error {
	return handler.DB.Model(table).Where(where).Updates(value).Error
}

// Delete
func (handler *DBHandler) DeleteByModel(model interface{}) (count int64, err error) {
	curdb := handler.DB.Delete(model)
	err = curdb.Error
	if err != nil {
		return
	}
	count = curdb.RowsAffected
	return
}

// Delete
func (handler *DBHandler) DeleteByWhere(model, where interface{}) (count int64, err error) {
	curdb := handler.DB.Where(where).Delete(model)
	err = curdb.Error
	if err != nil {
		return
	}
	count = curdb.RowsAffected
	return
}

// Delete
func (handler *DBHandler) DeleteByID(model interface{}, id uint64) (count int64, err error) {
	curdb := handler.DB.Where("id=?", id).Delete(model)
	err = curdb.Error
	if err != nil {
		return
	}
	count = curdb.RowsAffected
	return
}

// Delete
func (handler *DBHandler) DeleteByIDS(model interface{}, ids []uint64) (count int64, err error) {
	curdb := handler.DB.Where("id in (?)", ids).Delete(model)
	err = curdb.Error
	if err != nil {
		return
	}
	count = curdb.RowsAffected
	return
}

// First
func (handler *DBHandler) FirstByID(out interface{}, id int) (notFound bool, err error) {
	where := fmt.Sprintf("id = %v", id)
	err = handler.DB.First(out, where).Error
	if err != nil {
		notFound = gorm.IsRecordNotFoundError(err)
	}
	return
}

// First
func (handler *DBHandler) First(where interface{}, out interface{}, preloads ...string) (notFound bool, err error) {

	curdb := handler.DB.Where(where)
	//err = handler.DB.Where(where).First(out, where).Error
	if len(preloads) > 0 {
		for _, preload := range preloads {
			curdb = curdb.Preload(preload)
		}
	}

	err = curdb.First(out, where).Error
	if err != nil {
		notFound = gorm.IsRecordNotFoundError(err)
	}
	return
}

// Find
func (handler *DBHandler) Find(where interface{}, out interface{},
	orders, preloads []string) error {
	curdb := handler.DB.Where(where)
	if len(orders) > 0 {
		for _, order := range orders {
			curdb = curdb.Order(order)
		}
	}

	if len(preloads) > 0 {
		for _, preload := range preloads {
			curdb = curdb.Preload(preload)
		}
	}
	return curdb.Find(out).Error
}

// Find define select
func (handler *DBHandler) SelectFind(getContent interface{}, where interface{}, out interface{}, groupBy string,
	tableName string, orders ...string) error {
	curdb := handler.DB.Table(tableName).Select(getContent).Where(where)
	if groupBy != "" {

		curdb = curdb.Group(groupBy)

	}
	if len(orders) > 0 {
		for _, order := range orders {
			curdb = curdb.Order(order)
		}
	}
	return curdb.Find(out).Error
}

// Scan
func (handler *DBHandler) Scan(model, where interface{}, out interface{}) (notFound bool, err error) {
	err = handler.DB.Model(model).Where(where).Scan(out).Error
	if err != nil {
		notFound = gorm.IsRecordNotFoundError(err)
	}
	return
}

func (handler *DBHandler) ScanBySql(sql string, out interface{}) (notFound bool, err error) {
	err = handler.DB.Raw(sql).Scan(out).Error
	if err != nil {
		notFound = gorm.IsRecordNotFoundError(err)
	}
	return
}

// ScanList
func (handler *DBHandler) ScanList(model, where interface{}, out interface{}, orders ...string) error {
	curdb := handler.DB.Model(model).Where(where)
	if len(orders) > 0 {
		for _, order := range orders {
			curdb = curdb.Order(order)
		}
	}
	return curdb.Scan(out).Error
}

//
func (handler *DBHandler) ScanListTable(tableName string, where interface{}, out interface{}, orders ...string) error {
	curdb := handler.DB.Table(tableName)

	if where != nil {
		curdb = curdb.Where(where)
	}

	if len(orders) > 0 {
		for _, order := range orders {
			curdb = curdb.Order(order)
		}
	}
	return curdb.Scan(out).Error
}

// GetPage
func (handler *DBHandler) GetPage(model, where interface{}, out interface{}, pageIndex, pageSize uint64,
	totalCount *uint64, preload string, whereOrder ...PageWhereOrder) error {
	curdb := handler.DB.Preload(preload).Model(model).Where(where)
	if len(whereOrder) > 0 {
		for _, wo := range whereOrder {
			if wo.Order != "" {
				curdb = curdb.Order(wo.Order)
			}
			if wo.Where != "" {
				curdb = curdb.Where(wo.Where, wo.Value...)
			}
		}
	}
	err := curdb.Count(totalCount).Error
	if err != nil {
		return err
	}
	if *totalCount == 0 {
		return nil
	}
	return curdb.Offset((pageIndex - 1) * pageSize).Limit(pageSize).Find(out).Error
}

// PluckList
func (handler *DBHandler) PluckList(model, where interface{}, out interface{}, fieldName string) error {
	return handler.DB.Model(model).Where(where).Pluck(fieldName, out).Error
}

func (handler *DBHandler) HashCreateTable(model interface{}) {
	if !handler.DB.HasTable(model) {
		handler.DB.CreateTable(model)
	}
}

func (handler *DBHandler) Begin() (*gorm.DB, error) {
	tx := handler.DB.Begin()
	return tx, nil
}

func (handler *DBHandler) GetDB() *gorm.DB {
	return handler.DB
}

func (handler *DBHandler) AutoMigrate(v []interface{}) error {

	return handler.DB.AutoMigrate(v...).Error
}

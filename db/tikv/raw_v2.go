// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

type rawV2DB struct {
	db      *rawkv.Client
	r       *util.RowCodec
	bufPool *util.BufPool
}

func createRawV2DB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	db, err := rawkv.NewClientV2(context.Background(), strings.Split(pdAddr, ","), config.Security{})
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &rawV2DB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
	}, nil
}

func (db *rawV2DB) Close() error {
	return db.db.Close()
}

func (db *rawV2DB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rawV2DB) CleanupThread(ctx context.Context) {
}

func (db *rawV2DB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *rawV2DB) ToSqlDB() *sql.DB {
	return nil
}

func (db *rawV2DB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	row, err := db.db.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row, fields)
}

func (db *rawV2DB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	values, err := db.db.BatchGet(ctx, rowKeys)
	if err != nil {
		return nil, err
	}
	rowValues := make([]map[string][]byte, len(keys))

	for i, value := range values {
		if len(value) > 0 {
			rowValues[i], err = db.r.Decode(value, fields)
		} else {
			rowValues[i] = nil
		}
	}
	return rowValues, nil
}

func (db *rawV2DB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	_, rows, err := db.db.Scan(ctx, db.getRowKey(table, startKey), nil, count)
	if err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

func (db *rawV2DB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	row, err := db.db.Get(ctx, db.getRowKey(table, key))
	if err != nil {
		return nil
	}

	data, err := db.r.Decode(row, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	return db.Insert(ctx, table, key, data)
}

func (db *rawV2DB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		// TODO should we check the key exist?
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.BatchPut(ctx, rawKeys, rawValues)
}

func (db *rawV2DB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	currentTime := uint64(time.Now().Unix()) + 600
	return db.db.PutWithTTL(ctx, db.getRowKey(table, key), buf, currentTime)
	//return db.db.Put(ctx, db.getRowKey(table, key), buf)
}

func (db *rawV2DB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.BatchPut(ctx, rawKeys, rawValues)
}

func (db *rawV2DB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(ctx, db.getRowKey(table, key))
}

func (db *rawV2DB) BatchDelete(ctx context.Context, table string, keys []string) error {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	return db.db.BatchDelete(ctx, rowKeys)
}

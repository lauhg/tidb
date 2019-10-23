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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

type SelectIntoExec struct {
	baseExecutor

	selectIntoInfo *SelectIntoInfo
}

// Next implements the Executor Next interface.
func (e *SelectIntoExec) Next(ctx context.Context, req *chunk.Chunk) error {
	// req.GrowAndReset(e.maxChunkSize)

	sctx := e.selectIntoInfo.Ctx
	val := sctx.Value(SelectIntoVarKey)
	if val != nil {
		sctx.SetValue(SelectIntoVarKey, nil)
		panic("TODO")
	}
	if e.selectIntoInfo.FileName == "" {
		return errors.New("Select Into: filename is empty")
	}
	sctx.SetValue(SelectIntoVarKey, e.selectIntoInfo)

	return nil
}

// Close implements the Executor Close interface.
func (e *SelectIntoExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *SelectIntoExec) Open(ctx context.Context) error {
	return nil
}

// SelectIntoInfo saves the information of loading data operation.
type SelectIntoInfo struct {
	Ctx sessionctx.Context

	Tp         ast.SelectIntoType
	FileName   string
	FieldsInfo *ast.FieldsClause
	LinesInfo  *ast.LinesClause
}

// SelectIntoVarKeyType is a dummy type to avoid naming collision in context.
type SelectIntoVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k SelectIntoVarKeyType) String() string {
	return "select_into_var"
}

// SelectIntoVarKey is a variable key for load data.
const SelectIntoVarKey SelectIntoVarKeyType = 0

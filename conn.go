package awsdapisqldriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

type Conn struct {
	ctx           context.Context
	options       *options
	transactionID string // if in middle of tx
}

func newConn(ctx context.Context, options *options) *Conn {
	return &Conn{
		ctx:     ctx,
		options: options,
	}
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	input := rdsdataservice.BeginTransactionInput{
		Database:    c.options.database.Ptr(),
		ResourceArn: c.options.resourceArn.Ptr(),
		SecretArn:   c.options.secretArn.Ptr(),
		Schema:      c.options.schema.Ptr(),
	}
	output, err := c.options.dataServiceApi.BeginTransactionWithContext(ctx, &input)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx: %w", err)
	}

	c.transactionID = aws.StringValue(output.TransactionId)

	return &Tx{
		context: ctx,
		options: c.options,
		conn:    c,
	}, nil
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return executeStatement(ctx, c.options, query, c.transactionID, args...)
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(context.Background(), c.options, query), nil
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return newStmt(ctx, c.options, query), nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return executeStatement(ctx, c.options, query, c.transactionID, args...)
}

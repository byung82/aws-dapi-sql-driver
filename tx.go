package awsdapisqldriver

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

type Tx struct {
	context context.Context
	options *options
	conn    *Conn
}

func (t *Tx) Commit() error {
	input := rdsdataservice.CommitTransactionInput{
		ResourceArn:   t.options.resourceArn.Ptr(),
		SecretArn:     t.options.secretArn.Ptr(),
		TransactionId: aws.String(t.conn.transactionID),
	}
	if _, err := t.options.dataServiceApi.CommitTransactionWithContext(t.context, &input); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	t.conn.transactionID = ""

	return nil
}

func (t *Tx) Rollback() error {
	input := rdsdataservice.RollbackTransactionInput{
		ResourceArn:   t.options.resourceArn.Ptr(),
		SecretArn:     t.options.secretArn.Ptr(),
		TransactionId: aws.String(t.conn.transactionID),
	}
	if _, err := t.options.dataServiceApi.RollbackTransactionWithContext(t.context, &input); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}

	t.conn.transactionID = ""

	return nil
}

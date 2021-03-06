package awsdapisqldriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"gopkg.in/guregu/null.v4"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

type Result struct {
	timezone     *time.Location
	databaseType string
	output       *rdsdataservice.ExecuteStatementOutput
	records      [][]*rdsdataservice.Field
}

func (r *Result) Columns() []string {
	var columns []string
	for _, meta := range r.output.ColumnMetadata {
		columns = append(columns, aws.StringValue(meta.Name))
	}
	return columns
}

func (r *Result) Close() error {
	return nil
}

func (r *Result) Next(dest []driver.Value) error {
	if len(r.records) == 0 {
		return io.EOF
	}

	for index, field := range r.records[0] {
		v := valueOf(field)

		if s, ok := v.(string); ok {
			if s == "" {
				continue
			}

			if meta := r.output.ColumnMetadata; len(meta) > index {
				var layout string

				if r.databaseType == "postgres" {
					switch typeName := aws.StringValue(meta[index].TypeName); typeName {
					case "timestamp", "timestamptz", "TIMESTAMP", "TIMESTAMPTZ":
						layout = "2006-01-02 15:04:05.999999"
					}
				} else {
					switch typeName := aws.StringValue(meta[index].TypeName); typeName {
					case "DATE":
						layout = "2006-01-02"
					case "DATETIME", "TIMESTAMP":
						layout = "2006-01-02 15:04:05"
					case "YEAR":
						layout = "2006"
						if len(s) == 2 {
							layout = "06"
						}
					case "timestamp", "timestamptz":
						layout = "2006-01-02 15:04:05.999999"
					}
				}

				if layout != "" {
					t, err := time.Parse(layout, s)
					if err != nil {
						return fmt.Errorf("failed to parse time, %v: %w", s, err)
					}
					v = t
				}
			}
		}

		dest[index] = v
	}

	r.records = r.records[1:]

	return nil
}

func (r *Result) LastInsertId() (int64, error) {
	if len(r.output.GeneratedFields) == 0 {
		return 0, fmt.Errorf("last id not available")
	}

	return aws.Int64Value(r.output.GeneratedFields[0].LongValue), nil
}

func (r *Result) RowsAffected() (int64, error) {
	return aws.Int64Value(r.output.NumberOfRecordsUpdated), nil
}

const prefix = "f"

func executeStatement(ctx context.Context, options *options, query, transactionID string, args ...driver.NamedValue) (*Result, error) {
	if options.databaseType.Equal(null.StringFrom("postgres")) {
		query = strings.ReplaceAll(query, "$", ":"+prefix)
	} else {
		query = nameParameters(prefix, query)
	}

	input := &rdsdataservice.ExecuteStatementInput{
		ContinueAfterTimeout:  options.continueAfterTimeout.Ptr(),
		Database:              options.database.Ptr(),
		IncludeResultMetadata: aws.Bool(true),
		ResourceArn:           options.resourceArn.Ptr(),
		Schema:                options.schema.Ptr(),
		SecretArn:             options.secretArn.Ptr(),
		Sql:                   aws.String(query),
	}

	if transactionID != "" {
		input.TransactionId = aws.String(transactionID)
	}

	for _, arg := range args {
		name := arg.Name

		if name == "" {
			name = prefix + strconv.Itoa(arg.Ordinal)
		}

		param := rdsdataservice.SqlParameter{
			Name:  aws.String(name),
			Value: asField(arg.Value),
		}

		input.Parameters = append(input.Parameters, &param)
	}

	output, err := options.dataServiceApi.ExecuteStatementWithContext(ctx, input)

	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	return &Result{
		databaseType: options.databaseType.String,
		output:       output,
		records:      output.Records,
	}, nil
}

// nameParameters replaces ? in the query with named parameters.  required because
// the aws data api doesn't appear to support the ordinal ? parameters
//
// e.g. "select * from foo where id = ?" -> "select * from foo where id = :f1"
func nameParameters(prefix, query string) string {
	var got []rune
	var n int
	for _, r := range query {
		if r == '?' {
			n++
			got = append(got, ':')
			got = append(got, []rune(prefix)...)
			got = append(got, []rune(strconv.Itoa(n))...)
			continue
		}
		got = append(got, r)
	}
	return string(got)
}

package dynamodb

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/persistence"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

// No sort key is needed because we are only storing the latest state
type StateItem struct {
	PersistenceID string // Partition key
	VersionNumber uint64
	StatePayload  []byte
	StateManifest string
	Timestamp     int64
	ShardNumber   uint64
}

const (
	tableName = "states_store"
)

var onceAwsConfig sync.Once
var onceDdbClient sync.Once

// DynamoDurableStore implements the DurableStore interface
// and helps persist states in a DynamoDB
type DynamoDurableStore struct {
	client *dynamodb.Client
}

// enforce interface implementation
var _ persistence.StateStore = (*DynamoDurableStore)(nil)

// Connect connects to the journal store
func (d DynamoDurableStore) Connect(ctx context.Context) error {
	// Initialize DynamoDB client
	d.client = GetDynamodbClient()
	return nil
}

func getAwsConfig() aws.Config {
	onceAwsConfig.Do(func() {
		var err error
		awsConfig, err = awsConfigMod.LoadDefaultConfig(context.TODO())
		if err != nil {
			panic(err)
		}
	})

	return awsConfig
}

func GetDynamodbClient() *dynamodb.Client {
	onceDdbClient.Do(func() {
		awsConfig = getAwsConfig()

		region := config.Region

		dynamodbClient = dynamodb.NewFromConfig(awsConfig, func(opt *dynamodb.Options) {
			opt.Region = region
		})
	})

	return dynamodbClient
}

// Disconnect disconnect the journal store
// There is no need to disconnect because the client is stateless
func (DynamoDurableStore) Disconnect(ctx context.Context) error {
	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (d DynamoDurableStore) Ping(ctx context.Context) error {
	_, err := d.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("unable to connect to DynamoDB, %v", err)
	}
}

// WriteState persist durable state for a given persistenceID.
func (d DynamoDurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {

	bytea, _ := proto.Marshal(state.GetResultingState())
	manifest := string(state.GetResultingState().ProtoReflect().Descriptor().FullName())

	// Define the item to upsert
	item := map[string]types.AttributeValue{
		"PersistenceID": &types.AttributeValueMemberS{Value: state.GetPersistenceId()}, // Partition key
		"VersionNumber": &types.AttributeValueMemberS{Value: state.GetVersionNumber()},
		"StatePayload":  &types.AttributeValueMemberS{Value: bytea},
		"StateManifest": &types.AttributeValueMemberS{Value: manifest},
		"Timestamp":     &types.AttributeValueMemberS{Value: state.GetTimestamp()},
		"ShardNumber":   &types.AttributeValueMemberS{Value: state.GetShard()},
	}

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to upsert state into the dynamodb: %w", err)
	}

	return nil
}

// GetLatestState fetches the latest durable state
func (d DynamoDurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	// Get criteria
	key := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: persistenceID},
	}

	// Perform the GetItem operation
	resp, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the latest state from the dynamodb: %w", err)
	}

	// Check if item exists
	if resp.Item == nil {
		return nil, nil
	}

	// unmarshal the event and the state
	state, err := toProto(resp.StateManifest, resp.StatePayload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal the durable state: %w", err)
	}

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  resp.VersionNumber,
		ResultingState: state,
		Timestamp:      resp.Timestamp,
		Shard:          resp.ShardNumber,
	}, nil
}

// toProto converts a byte array given its manifest into a valid proto message
func toProto(manifest string, bytea []byte) (*anypb.Any, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(manifest))
	if err != nil {
		return nil, err
	}

	pm := mt.New().Interface()
	err = proto.Unmarshal(bytea, pm)
	if err != nil {
		return nil, err
	}

	if cast, ok := pm.(*anypb.Any); ok {
		return cast, nil
	}
	return nil, fmt.Errorf("failed to unpack message=%s", manifest)
}

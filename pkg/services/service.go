package services

import (
	"context"

	"github.com/micro-tok/discover-service/pkg/cassandra"
	"github.com/micro-tok/discover-service/pkg/pb"
	"github.com/micro-tok/discover-service/pkg/redis"
)

type Service interface {
	DiscoverService
}

type service struct {
	DiscoverService
}

type DiscoverService interface {
	DiscoverFeed(ctx context.Context, req *pb.DiscoverFeedRequest) (*pb.DiscoverFeedResponse, error)
	DiscoverFeedWithTags(ctx context.Context, req *pb.DiscoverFeedWithTagsRequest) (*pb.DiscoverFeedResponse, error)
}

type discoverService struct {
	cass  *cassandra.CassandraService
	redis *redis.RedisService
}

func NewDiscoverService(cass *cassandra.CassandraService, redis *redis.RedisService) Service {
	return &service{
		DiscoverService: &discoverService{
			cass:  cass,
			redis: redis,
		},
	}
}

func (s discoverService) DiscoverFeed(ctx context.Context, req *pb.DiscoverFeedRequest) (*pb.DiscoverFeedResponse, error) {
	metadata, err := s.cass.LoadMetadataAll()
	if err != nil {
		return nil, err
	}

	return &pb.DiscoverFeedResponse{
		Items: metadata,
	}, nil

}

func (s discoverService) DiscoverFeedWithTags(ctx context.Context, req *pb.DiscoverFeedWithTagsRequest) (*pb.DiscoverFeedResponse, error) {
	metadata, err := s.cass.LoadMetadataByTags(req.Tags)
	if err != nil {
		return nil, err
	}

	return &pb.DiscoverFeedResponse{
		Items: metadata,
	}, nil
}

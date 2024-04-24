package cassandra

import (
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/micro-tok/discover-service/pkg/config"
	"github.com/micro-tok/discover-service/pkg/pb"
)

type CassandraService struct {
	ClusterIP string
	Keyspace  string
}

func NewCassandraService(cfg *config.Config) *CassandraService {
	return &CassandraService{
		ClusterIP: cfg.CassandraClusterIP,
		Keyspace:  cfg.CassandraKeyspace,
	}
}

func (s CassandraService) LoadMetadataAll() ([]*pb.DiscoverFeedItem, error) {
	cluster := gocql.NewCluster(s.ClusterIP)
	cluster.Keyspace = s.Keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
		return nil, err
	}
	defer session.Close()

	var videos []*pb.DiscoverFeedItem

	query := session.Query(`SELECT video_id, owner_id, title, description, url, tags, created_at FROM videos ORDER BY created_at DESC`)
	iter := query.Iter()
	scanner := iter.Scanner()

	for scanner.Next() {
		var video_id, owner_id, title, description, url string
		var tags []string
		var created_at time.Time

		err = scanner.Scan(&video_id, &owner_id, &title, &description, &url, &tags, &created_at)
		if err != nil {
			log.Fatalf("Failed to scan video: %v", err)
			return nil, err
		}

		video := &pb.DiscoverFeedItem{
			VideoId:     video_id,
			OwnerId:     owner_id,
			Title:       title,
			Description: description,
			Url:         url,
			Tags:        tags,
			CreatedAt:   created_at.String(),
		}

		videos = append(videos, video)
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Failed to scan videos: %v", err)
		return nil, err
	}

	return videos, nil
}

func (s CassandraService) LoadMetadataByTags(tags []string) ([]*pb.DiscoverFeedItem, error) {
	cluster := gocql.NewCluster(s.ClusterIP)
	cluster.Keyspace = s.Keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
		return nil, err
	}
	defer session.Close()

	var videos []*pb.DiscoverFeedItem

	for _, tag := range tags {

		query := session.Query(`SELECT video_id, owner_id, title, description, url, tags, created_at FROM videos WHERE tags CONTAINS ? ORDER BY created_at DESC`, tag)
		iter := query.Iter()
		scanner := iter.Scanner()

		for scanner.Next() {
			var video_id, owner_id, title, description, url string
			var tags []string
			var created_at time.Time

			err = scanner.Scan(&video_id, &owner_id, &title, &description, &url, &tags, &created_at)
			if err != nil {
				log.Fatalf("Failed to scan video: %v", err)
				return nil, err
			}

			video := &pb.DiscoverFeedItem{
				VideoId:     video_id,
				OwnerId:     owner_id,
				Title:       title,
				Description: description,
				Url:         url,
				Tags:        tags,
				CreatedAt:   created_at.String(),
			}

			videos = append(videos, video)
		}

		if err := scanner.Err(); err != nil {
			log.Fatalf("Failed to scan videos: %v", err)
			return nil, err
		}
	}

	return videos, nil
}

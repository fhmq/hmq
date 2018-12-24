package sessions

import (
	"time"

	log "github.com/cihub/seelog"
	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
)

var redisClient *redis.Client
var _ SessionsProvider = (*redisProvider)(nil)

const (
	sessionName = "session"
)

type redisProvider struct {
}

func init() {
	Register("redis", NewRedisProvider())
}

func InitRedisConn(url string) {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	err := redisClient.Ping().Err()
	for err != nil {
		log.Error("connect redis error: ", err, " 3s try again...")
		time.Sleep(3 * time.Second)
		err = redisClient.Ping().Err()
	}
}

func NewRedisProvider() *redisProvider {
	return &redisProvider{}
}

func (r *redisProvider) New(id string) (*Session, error) {
	val, _ := jsoniter.Marshal(&Session{id: id})

	err := redisClient.HSet(sessionName, id, val).Err()
	if err != nil {
		return nil, err
	}

	result, err := redisClient.HGet(sessionName, id).Bytes()
	if err != nil {
		return nil, err
	}

	sess := Session{}
	err = jsoniter.Unmarshal(result, &sess)
	if err != nil {
		return nil, err
	}

	return &sess, nil
}

func (r *redisProvider) Get(id string) (*Session, error) {

	result, err := redisClient.HGet(sessionName, id).Bytes()
	if err != nil {
		return nil, err
	}

	sess := Session{}
	err = jsoniter.Unmarshal(result, &sess)
	if err != nil {
		return nil, err
	}

	return &sess, nil
}

func (r *redisProvider) Del(id string) {
	redisClient.HDel(sessionName, id)
}

func (r *redisProvider) Save(id string) error {
	return nil
}

func (r *redisProvider) Count() int {
	return int(redisClient.HLen(sessionName).Val())
}

func (r *redisProvider) Close() error {
	return redisClient.Del(sessionName).Err()
}

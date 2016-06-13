package callosum

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3" //sqllite DB driver import
)

//UserRow holds the data obtained from fetching a row from the `users` table.
type UserRow struct {
	ID               int64
	ScreenName       string
	Description      string
	LastLookedAt     string
	LatestTweetID    int64
	LatestFriendID   int64
	LatestFollowerID int64
	Protected        int
	Processed        int
	Accepted         int
	Blob             []byte
}

//TweetRow holds the data obtained from fetching a row from the `tweets` table
type TweetRow struct {
	TweetID    int64
	CreatedAt  string
	Language   string
	screenName string
	tweet      []byte
}

//Storage holds a open connection the the sqlite database
type Storage struct {
	db *sql.DB
}

type queryArgs struct {
	query string
	args  []interface{}
}

var mutex = &sync.Mutex{}

var chQueryArgs chan *queryArgs

var db *sql.DB

func executeStatements() {
	for {
		if qa, ok := <-chQueryArgs; ok {
			_, err := db.Exec(qa.query, qa.args...)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

//NewStorage creates returns a new Storage object.
//DBName is the name of the sqllite database file where
//all the users and tweets data will be collected. NewStorage
//create the sqlite file, if it is not already present and creates
//the tables. if the database is present, opens a connection.
func NewStorage(DBName string) *Storage {
	s := &Storage{}
	mutex.Lock()
	if db == nil {
		s.checkMakeDatabase(DBName)
		db = s.db
		if chQueryArgs == nil {
			chQueryArgs = make(chan *queryArgs, 100)
			go executeStatements()
		}

		s.setupTables()
	}
	mutex.Unlock()
	return s
}

func (s *Storage) setupTables() {
	tableName := "users"
	s.makeTable(tableName, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
							user_id INTEGER PRIMARY KEY,
							screen_name TEXT CONSTRAINT uniquescreenname UNIQUE,
							description TEXT CONSTRAINT defaultdesc DEFAULT "",
							last_looked_at INTEGER CONSTRAINT defaultlastlookedat DEFAULT 0,
							latest_tweet_id INTEGER CONSTRAINT defaultlatesttweetid DEFAULT 0,
							latest_following_id INTEGER CONSTRAINT defaultlatestfollowingid DEFAULT 0,
							latest_follower_id INTEGER CONSTRAINT defaultlatestfollowerid DEFAULT 0,
							protected INTEGER CONSTRAINT defaultprotected DEFAULT 0,
							processed INTEGER CONSTRAINT defaultprocessed DEFAULT 0,
							accepted INTEGER CONSTRAINT defaultaccepted DEFAULT 0,
							blob BLOB)`, tableName))
	tableName = "tweets"
	s.makeTable(tableName, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(tweet_id INTEGER PRIMARY KEY,
							created_at INTEGER,
							langugage TEXT,
							user_id INTEGER,
							desc TEXT,
							blob BLOB
							-- FOREIGN KEY(screen_name) REFERENCES users(screen_name)
							)`, tableName))

	tableName = "screennames"
	s.makeTable(tableName, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(screen_name TEXT PRIMARY KEY,
			processed INTEGER CONSTRAINT defaultprocessed DEFAULT 0)`, tableName))

	tableName = "userids"
	s.makeTable(tableName, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(user_id INTEGER PRIMARY KEY,
			processed INTEGER CONSTRAINT defaultprocessed DEFAULT 0)`, tableName))
	tableName = "followers"
	s.makeTable(tableName, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(user_id INTEGER,
			follower_id INTEGER,
			CONSTRAINT uniquemap UNIQUE (user_id, follower_id))`, tableName))
	tableName = "following"
	s.makeTable(tableName, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s(user_id INTEGER,
			following_id INTEGER,
			CONSTRAINT uniquemap UNIQUE (user_id, following_id))`, tableName))
}

func (s *Storage) checkMakeDatabase(DBName string) *sql.DB {
	var db *sql.DB
	db, err := sql.Open("sqlite3", DBName+".db") //?cache=shared&mode=rwc")
	if err != nil {
		log.Fatal(err)
	}

	db.Exec("PRAGMA journal_mode=WAL;")

	s.db = db
	return db
}

func (s *Storage) makeTable(tableName, sqlStmt string) {
	_, err := s.db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
		return
	}
}

//StoreScreenName inserts the given screenName into the `screenames` table
func (s *Storage) StoreScreenName(screenName string) {
	_, err := s.db.Exec("INSERT OR IGNORE INTO screennames (screen_name) VALUES (?)", screenName)
	if err != nil {
		log.Fatal(err)
	}
}

//StoreUser inserts the Twitter user details into the `users` table.
func (s *Storage) StoreUser(userID int64, screenName, description string, protected bool, blob []byte) {
	chQueryArgs <- &queryArgs{"INSERT OR IGNORE INTO users (user_id, screen_name, description, protected, blob) VALUES (?, ?, ?, ?, ?)",
		[]interface{}{userID, screenName, description, protected, blob}}
}

//StoreTweet inserts the tweet details into the `tweets` table.
func (s *Storage) StoreTweet(tweetID, createdAt, userID int64, language, desc string, blob []byte) {
	chQueryArgs <- &queryArgs{"INSERT OR IGNORE INTO tweets (tweet_id, created_at, langugage, user_id, desc, blob) VALUES (?, ?, ?, ?, ?, ?)",
		[]interface{}{tweetID, createdAt, language, userID, desc, blob}}
}

func (s *Storage) storeFriendOrFollower(userID, friendOrFollowerID int64, query string) {
	chQueryArgs <- &queryArgs{query, []interface{}{userID, friendOrFollowerID}}
}

//StoreFriends stores the mapping between the userID and the IDs of
//users the follow into the `following` table.
func (s *Storage) StoreFriends(userID int64, friendIDs []int64) {
	for _, friendID := range friendIDs {
		s.storeFriendOrFollower(userID, friendID, "INSERT OR IGNORE INTO following (user_id, following_id) VALUES (?, ?)")
	}
}

//StoreFollowers stores the mapping between the userID and the IDs of
//their followes into the `followers` table.
func (s *Storage) StoreFollowers(userID int64, followerIDs []int64) {
	for _, followerID := range followerIDs {
		s.storeFriendOrFollower(userID, followerID, "INSERT OR IGNORE INTO followers (user_id, follower_id) VALUES (?, ?)")
	}
}

func (s *Storage) storeUserID(userID int64) {
	chQueryArgs <- &queryArgs{"INSERT OR IGNORE INTO userids (user_id) VALUES (?)", []interface{}{userID}}
}

//StoreUserIDs stores the given userIDs in the `userids` table
func (s *Storage) StoreUserIDs(userIDs []int64) {
	for _, userID := range userIDs {
		s.storeUserID(userID)
	}
}

func (s *Storage) queryScreenNamesOrIDs(query string, results interface{}) {
	rows, err := s.db.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {
		switch x := results.(type) {
		case *[]string:
			var item string
			rows.Scan(&item)
			*x = append(*x, item)
		case *[]int64:
			var item int64
			rows.Scan(&item)
			*x = append(*x, item)
		default:
			log.Fatal("results type must be *[]string or *[]int64")
		}
	}
}

//GetScreenNames gets Twitter handles from the `screenames` table that have already been processed
func (s *Storage) GetScreenNames() []string {
	var results []string
	s.queryScreenNamesOrIDs("SELECT screen_name from screennames where processed=1", &results)
	return results
}

//GetUnprocessedScreenNames gets Twitter handles from the `screenames` table that are yet to be processed
func (s *Storage) GetUnprocessedScreenNames() []string {
	var results []string
	s.queryScreenNamesOrIDs("SELECT screen_name from screennames where processed=0", &results)
	return results
}

//GetUserIDs gets user ids from the `userids` table that have already been processed
func (s *Storage) GetUserIDs() []int64 {
	var results []int64
	s.queryScreenNamesOrIDs("SELECT user_id from userids where processed=1", &results)
	return results
}

//GetUnprocessedUserIDs gets user ids from the `userids` table that are yet to be processed
func (s *Storage) GetUnprocessedUserIDs() []int64 {
	var results []int64
	s.queryScreenNamesOrIDs("SELECT user_id from userids where processed=0", &results)
	return results
}

//GetAcceptedUserIDs gets user ids from the `users` table for whom the user filtering
//function has marked them as accepted for further processing
func (s *Storage) GetAcceptedUserIDs() []int64 {
	var results []int64
	s.queryScreenNamesOrIDs("SELECT user_id from users where accepted=1", &results)
	return results
}

//GetUserByScreenNameOrID gets the UserRow for the given screenName or ID
func (s *Storage) GetUserByScreenNameOrID(screenNameOrID interface{}) *UserRow {
	var u UserRow
	query := `SELECT user_id, 
					 screen_name,
					 description,
					 last_looked_at,
					 latest_tweet_id,
					 latest_following_id,
					 latest_follower_id,
					 protected,
					 processed,
					 accepted,
					 blob
				FROM users
				WHERE %s=?`

	var row *sql.Row

	switch x := screenNameOrID.(type) {
	case int64:
		query = fmt.Sprintf(query, "user_id")
		row = s.db.QueryRow(query, x)
	case string:
		query = fmt.Sprintf(query, "screen_name")
		row = s.db.QueryRow(query, x)
	}

	err := row.Scan(
		&u.ID,
		&u.ScreenName,
		&u.Description,
		&u.LastLookedAt,
		&u.LatestTweetID,
		&u.LatestFriendID,
		&u.LatestFollowerID,
		&u.Protected,
		&u.Processed,
		&u.Accepted,
		&u.Blob)

	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		log.Fatal(err)
	}
	return &u

}

//MarkUserLatestTweetsCollected updates the `last_looked_at` timestamp and the `latest_tweet_id` for
//the given user in the `users` table
func (s *Storage) MarkUserLatestTweetsCollected(userID int64, lastLookedAt, latestTweetID int64) {
	chQueryArgs <- &queryArgs{"UPDATE users SET last_looked_at=?, latest_tweet_id=? where user_id=?", []interface{}{lastLookedAt, latestTweetID, userID}}
}

//MarkUserLatestFriendsCollected sets the `latest_following_id` to the latest id of the users given userID
//is following
func (s *Storage) MarkUserLatestFriendsCollected(userID, latestFriendID int64) {
	chQueryArgs <- &queryArgs{"UPDATE users SET latest_following_id=? where user_id=?", []interface{}{latestFriendID, userID}}
}

//MarkUserLatestFollowersCollected sets the `latest_follower_id` to the latest id of the followers collected
func (s *Storage) MarkUserLatestFollowersCollected(userID, latestFollowerID int64) {
	chQueryArgs <- &queryArgs{"UPDATE users SET latest_follower_id=? where user_id=?", []interface{}{latestFollowerID, userID}}
}

//MarkUserProcessed sets the `processed` and the `accepted` flags for the user in the `users` table
func (s *Storage) MarkUserProcessed(ID int64, processed, accepted bool) {
	chQueryArgs <- &queryArgs{"UPDATE users SET processed=?, accepted=? where user_id=?", []interface{}{processed, accepted, ID}}
}

//MarkUserIDProcessed sets the `processed` flag for the given user id in the `userids` table
func (s *Storage) MarkUserIDProcessed(ID int64, processed bool) {
	chQueryArgs <- &queryArgs{"UPDATE userids SET processed=? where user_id=?", []interface{}{processed, ID}}
}

//MarkScreenNameProcessed sets the `processed` flag for the given screenName in the `screennames` table
func (s *Storage) MarkScreenNameProcessed(screenName string, processed bool) {
	chQueryArgs <- &queryArgs{"UPDATE screennames SET processed=? where screen_name=?", []interface{}{processed, screenName}}
}

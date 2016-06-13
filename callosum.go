//Package callosum is a go package that helps build a corpus or collection from Twitter
//by recursively getting Twitter users and their tweets, friends, and followers.
//
//users and tweets collection can be stopped and restarted anytime. callosum picks
// up from where it left off.
package callosum

import "time"

type listGetter func(interface{}, int64) ([]int64, int64)

//FilterUser is any function that takes in a byte blob with twitter's JSON response
//for a user and returns true if the user matches the filtering criteria. A true will
//lead to the user being consider for collecting their tweets, friends and followers
type FilterUser func(blob []byte) bool

func trimTillID(IDs []int64, seenID int64) ([]int64, bool) {
	var trimmedIDs []int64
	var present bool
	for _, ID := range IDs {
		if ID == seenID {
			present = true
			break
		}
		trimmedIDs = append(trimmedIDs, ID)
	}
	return trimmedIDs, present
}

//TwitterCollector provides a library of methods that help get tweets, friends, followers
//of twitter users and also store them to the database
//
//Get* methods uses twitter's API to get objects from twitter. They fetch all new objects for
//a user since a specific point (for example: GetTweets gets all tweets given the latest tweet
//ID we have for a user)
//
//Collect* methods both get the objects and also write them to the database.
type TwitterCollector struct {
	n          *Network
	s          *Storage
	filterUser FilterUser
}

//NewTwitterCollector returns a new Twitter Collector.
//
//A sqlite database is created with DBName as the file name.
//
//authFileName and window are parameters needed for using Twitter's API.
//
//authFileName contains the authentication tokens you get from Twitter's
//Application Management portal - apps.twitter.com, Checkout template_auth.json for the format
//
//window specifies Twitter's rate limit rollover window
//
//fu specifies a filter function that takes the byte blob with Twitter's JSON response for a user object lookup
//and returns true if the user meets the criteron to follow up to get their tweets and their friends and followers.
func NewTwitterCollector(DBName, authFileName string, window time.Duration, fu FilterUser) *TwitterCollector {
	t := &TwitterCollector{}
	t.n = NewNetwork(authFileName, window)
	t.s = NewStorage(DBName)
	t.filterUser = fu
	return t
}

func (t *TwitterCollector) getRelatedUsers(screenNameOrID interface{}, getter listGetter, lastUserID int64) []int64 {
	var cursorID int64 = -1
	var userIDs []int64
	for {
		var IDs []int64
		IDs, cursorID = getter(screenNameOrID, cursorID)
		if len(IDs) == 0 {
			break
		}
		IDs, trimmed := trimTillID(IDs, lastUserID)
		userIDs = append(userIDs, IDs...)
		if trimmed || cursorID == 0 {
			break
		}
	}
	return userIDs
}

//GetTweets gets all the Tweets from the timeline for a given screenNameOrID, starting from the latestTweetID.
//set latestTweetID to 0 to get all Tweets constrained by Twitter's max. limit
func (t *TwitterCollector) GetTweets(screenNameOrID interface{}, latestTweetID int64) Tweets {
	var allTweets Tweets
	var maxID int64

	for {
		tweets := t.n.GetUserTimeline(screenNameOrID, maxID)

		if len(tweets) == 0 {
			break
		}

		maxID = tweets[len(tweets)-1].ID //the array is sorted from most recent to least recent tweet
		tweets = tweets.trimTillID(latestTweetID)
		allTweets = append(allTweets, tweets...)

		if !(maxID > latestTweetID) {
			break
		}
	}
	return allTweets
}

//GetFriends gets the IDs of all Twitter users screenNameOrID is following, stopping at latestFriendID.
//set latestFriendID to 0 to get all the friends.
func (t *TwitterCollector) GetFriends(screenNameOrID interface{}, latestFriendID int64) []int64 {
	return t.getRelatedUsers(screenNameOrID, t.n.GetFriendIDs, latestFriendID)
}

//GetFollowers gets the IDs of Twitter users following screenNameOrID, stopping at latestFollowerID.
//set latestFollowerID to 0 to get all followers
func (t *TwitterCollector) GetFollowers(screenNameOrID interface{}, latestFollowerID int64) []int64 {
	return t.getRelatedUsers(screenNameOrID, t.n.GetFollowerIDs, latestFollowerID)
}

//CollectFriends gets all Twitter users that userID is following, stopping at latestFriendID
//and stores the mapping between the userID and the friendID for all friends in the
//`following` table, addes the followingIDs to the queue of users ids to be processed,
//in the `userids` table and updates the `latest_following_id` column in the `users` table.
func (t *TwitterCollector) CollectFriends(userID int64, latestFriendID int64) {
	friends := t.GetFriends(userID, latestFriendID)
	t.s.StoreFriends(userID, friends)
	t.s.StoreUserIDs(friends)
	t.s.MarkUserLatestFriendsCollected(userID, latestFriendID)
}

//CollectFollowers gets all Twitter followers of userID, stopping at latestFollowerID
//and stores the mapping between the userID and the follower for all followers in the
//`followers` table, adds the follower IDs to the queue of user ids to be processed,
//in the `userids` table and updates the `latest_follower_id` column  in the `users` table.
func (t *TwitterCollector) CollectFollowers(userID int64, latestFollowerID int64) {
	followers := t.GetFollowers(userID, latestFollowerID)
	t.s.StoreFollowers(userID, followers)
	t.s.StoreUserIDs(followers)
	t.s.MarkUserLatestFollowersCollected(userID, latestFollowerID)
}

//CollectUser gets the user from Twitter for the given screenNameOrID and stores
//the user in the `users` table. For users without protected tweets, applies the
//given filter function and applies the return truth value to the `accepted`
//table while also setting the `processed` column to mark the user as processed.
func (t *TwitterCollector) CollectUser(screenNameOrID interface{}) {
	u := t.n.GetUser(screenNameOrID)
	t.s.StoreUser(u.ID, u.Name, u.Description, u.Protected, u.Blob)
	if !u.Protected {
		t.s.MarkUserProcessed(u.ID, true, t.filterUser(u.Blob))
	}
}

//CollectTweets gets all the tweets of userID from Twitter, since the latestTweetID
//and updates the `last_looked_at` timestamp and the `latest_tweet_id` for the user.
func (t *TwitterCollector) CollectTweets(userID, latestTweetID int64) {
	tweets := t.GetTweets(userID, latestTweetID)
	for index, tweet := range tweets {
		t.s.StoreTweet(tweet.ID, tweet.CreatedAtTime().Unix(), userID, tweet.Language, tweet.Text, tweet.Blob)
		if index == 0 { //the first tweet in the list is the latest tweet from the user
			t.s.MarkUserLatestTweetsCollected(userID, time.Now().UTC().Unix(), tweet.ID)
		}
	}
}

//SeedScreenNames inserts the given Twitter screenNames into `screennames` table
//which is picked up later for processing.
func (t *TwitterCollector) SeedScreenNames(screenNames []string) {
	for _, screenName := range screenNames {
		t.s.StoreScreenName(screenName)
	}
}

//ProcessScreenNames gets screenNames from the `screennames` tables with
//the `processed` column not set and gets those users from Twitter, stores
//them in the `users` table and sets the `processed` column.
func (t *TwitterCollector) ProcessScreenNames() {
	screenNames := t.s.GetUnprocessedScreenNames()
	for _, screenName := range screenNames {
		u := t.s.GetUserByScreenNameOrID(screenName)
		if u == nil {
			t.CollectUser(screenName)
		}
		t.s.MarkScreenNameProcessed(screenName, true)
	}
}

//CollectAllUsers gets all the userIDs queued up for processing
//in the `userids` table, gets the users in batches and stores them
//in the users table and sets the `processed` column for those user IDs.
func (t *TwitterCollector) CollectAllUsers() {

	userIDs := t.s.GetUnprocessedUserIDs()
	filteredIDs := userIDs[:0]

	for _, ID := range userIDs {
		u := t.s.GetUserByScreenNameOrID(ID)
		if u == nil {
			filteredIDs = append(filteredIDs, ID)
		} else {
			t.s.MarkUserIDProcessed(ID, true)
		}
	}

	var chunk []int64
	chunkSize := 100

OuterLoop:

	for {
		switch x := len(filteredIDs); {
		case x == 0:
			break OuterLoop
		case x > chunkSize:
			chunk = filteredIDs[:chunkSize]
			filteredIDs = filteredIDs[chunkSize:]
		default:
			chunk = filteredIDs[:len(filteredIDs)]
			filteredIDs = filteredIDs[len(filteredIDs):]
		}

		users := t.n.GetUsers(chunk)
		for _, u := range users {
			t.s.StoreUser(u.ID, u.Name, u.Description, u.Protected, u.Blob)
			if !u.Protected {
				t.s.MarkUserProcessed(u.ID, true, t.filterUser(u.Blob))
			}
		}

		for _, ID := range chunk {
			t.s.MarkUserIDProcessed(ID, true)
		}
	}
}

//CollectAllFriends gets the user IDs marked as `accepted` in the
//users table by the filter function and collects all their Twitter
//friends (people they are following) and stores them in the database
func (t *TwitterCollector) CollectAllFriends() {
	for _, userID := range t.s.GetAcceptedUserIDs() {
		u := t.s.GetUserByScreenNameOrID(userID)
		t.CollectFriends(u.ID, u.LatestFriendID)
	}
}

//CollectAllFollowers gets the user IDs marked as `accepted` in the
//users table by the filter function and collects all their Twitter
//followers and stores them in the database
func (t *TwitterCollector) CollectAllFollowers() {
	for _, userID := range t.s.GetAcceptedUserIDs() {
		u := t.s.GetUserByScreenNameOrID(userID)
		t.CollectFollowers(u.ID, u.LatestFollowerID)
	}
}

//CollectAllTweets gets the user IDs marked as `accepted` in the
//users table by the filter function and collects all their tweets
//and stores them in the database
func (t *TwitterCollector) CollectAllTweets() {
	for _, userID := range t.s.GetAcceptedUserIDs() {
		u := t.s.GetUserByScreenNameOrID(userID)
		t.CollectTweets(u.ID, u.LatestTweetID)
	}
}

//StartCollection first processes any seeded screenames in the
//`screennames` table by getting and storing the users and
//repeatedly gets all the friends, followers and their tweets.
//By repeating, it picks up any new friends, followers from the
//`userids` table and futhers collection of their friends, followers,
//tweets. Stop collection any time by exiting the program.
func (t *TwitterCollector) StartCollection() {
	t.ProcessScreenNames()

	go Repeat(t.CollectAllFriends, 2*time.Second)
	go Repeat(t.CollectAllFollowers, 2*time.Second)
	go Repeat(t.CollectAllUsers, 2*time.Second)
	go Repeat(t.CollectAllTweets, 2*time.Second)
	c := make(chan struct{})
	<-c
}

//Repeat is a utility function to make sure a given function
//is periodically called.
func Repeat(processor func(), duration time.Duration) {
	for {
		start := time.Now()

		processor()

		if time.Since(start) < duration {
			time.Sleep(start.Add(duration).Sub(time.Now()))
		}
	}
}

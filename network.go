package callosum

import (
	"encoding/json"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/venkat/kuruvi"
)

//Tweet holds a tweet and exposes from fields in a tweet.
//Blob contains the entire tweet in JSON.
type Tweet struct {
	ID        int64  `json:"id"`
	Text      string `json:"text"`
	CreatedAt string `json:"created_at"`
	Language  string `json:"lang"`
	Blob      []byte
}

//CreatedAtTime is a wrapper to simplify parsing
//the CreatedAt timestamp
func (tweet *Tweet) CreatedAtTime() time.Time {
	var t time.Time
	t, err := time.Parse(time.RubyDate, tweet.CreatedAt)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

//User holds a Twitter user object and exposes some fields.
//Blob contains the entire user object as JSON.
type User struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	ScreenName  string `json:"screen_name"`
	Description string `json:"description"`
	LatestTweet Tweet  `json:"status"`
	Protected   bool   `json:"protected"`
	Blob        []byte
}

//Network holds a reference to the Twitter API client, Kuruvi
type Network struct {
	k *kuruvi.Kuruvi
}

//NewNetwork creates a new Network object. authFileName has the authentication
//information for Twitter's client. see template_auth.json for a sample.
//window is the rate limit window used by twitter (currently 15 mins)
func NewNetwork(authFileName string, window time.Duration) *Network {
	n := &Network{}

	authFile := getFile("auth.json")

	n.k = kuruvi.SetupKuruvi(
		window,
		kuruvi.GetAuthKeys(authFile),
		kuruvi.UseBoth)

	return n
}

//helper function to get an open file handle
func getFile(fileName string) *os.File {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

//Tweets is type for the list of Tweet obect
type Tweets []*Tweet

func (tweets Tweets) trimTillID(latestTweetID int64) Tweets {
	trimmedTweets := tweets[:0]
	for _, tweet := range tweets {
		if tweet.ID > latestTweetID {
			trimmedTweets = append(trimmedTweets, tweet)
		} else {
			break
		}
	}

	return trimmedTweets
}

//GetUserTimeline makes one API request to the user's timeline and sets max_id if
//maxID is not 0, which specifies the cursor position on the timeline. Consult
//Twiter's API documentation on user timeline for more details.
func (n *Network) GetUserTimeline(screenNameOrID interface{}, maxID int64) Tweets {
	v := url.Values{}

	n.addscreenNameOrID(&v, screenNameOrID)
	v.Add("trim_user", "true")
	v.Add("count", "200")
	if maxID != 0 {
		v.Add("max_id", strconv.FormatInt(maxID-1, 10))
	}
	var tweets []*Tweet
	data, err := n.k.Get("statuses/user_timeline", v)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(data, &tweets)
	if err != nil {
		log.Fatal(err, data)
	}

	var blobs []json.RawMessage
	err = json.Unmarshal(data, &blobs)
	if err != nil {
		log.Fatal(err, data)
	}
	for index, blob := range blobs {
		tweets[index].Blob = blob
	}
	return tweets
}

func (n *Network) addscreenNameOrID(v *url.Values, screenNameOrID interface{}) {
	switch x := screenNameOrID.(type) {
	case string:
		v.Add("screen_name", x)
	case int64:
		v.Add("user_id", strconv.FormatInt(x, 10))
	default:
		log.Fatal("screenNameOrID needs to a string or int64")
	}
}

//GetUser makes one API request to get a User from Twitter.
func (n *Network) GetUser(screenNameOrID interface{}) *User {
	var u *User

	v := url.Values{}
	n.addscreenNameOrID(&v, screenNameOrID)

	data, err := n.k.Get("users/show", v)
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(data, &u)
	var blob json.RawMessage
	err = json.Unmarshal(data, &blob)
	if err != nil {
		log.Fatal(err, data)
	}
	u.Blob = blob
	return u
}

//GetUsers makes an API request to get the User objects for
//given IDs. The API limits the number of IDs in a batch
//to 200
func (n *Network) GetUsers(IDs []int64) []*User {
	var users []*User

	v := url.Values{}
	IDStrings := make([]string, len(IDs))
	for index := range IDs {
		IDStrings[index] = strconv.FormatInt(IDs[index], 10)
	}
	v.Add("user_id", strings.Join(IDStrings, ","))
	data, err := n.k.Get("users/lookup", v)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(data, &users)
	if err != nil {
		log.Fatal(err, data)
	}

	var blobs []json.RawMessage
	err = json.Unmarshal(data, &blobs)
	if err != nil {
		log.Fatal(err, data)
	}
	for index, blob := range blobs {
		users[index].Blob = blob
		if users[index].Blob == nil || len(users[index].Blob) == 0 {
			log.Fatal("empty user blog", users[index])
		}
	}
	return users
}

func (n *Network) getUserIDs(screenNameOrID interface{}, endpoint string, cursorID int64) ([]int64, int64) {
	if cursorID == 0 {
		return []int64{}, 0
	}

	v := url.Values{}
	n.addscreenNameOrID(&v, screenNameOrID)
	v.Add("cursor", strconv.FormatInt(cursorID, 10))
	data, err := n.k.Get(endpoint, v)
	if err != nil {
		log.Fatal(err)
	}
	var result struct {
		IDs        []int64 `json:"ids"`
		NextCursor int64   `json:"next_cursor"`
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		log.Fatal(err, data)
	}
	return result.IDs, result.NextCursor
}

//GetFriendIDs gets the IDs of people that screenNameOrID is following. cursorID specifies
//the cursor position for multiple request. Please refer to Twitter's API documentation on
//cursoring for more details.
func (n *Network) GetFriendIDs(screenNameOrID interface{}, cursorID int64) ([]int64, int64) {
	return n.getUserIDs(screenNameOrID, "friends/ids", cursorID)
}

//GetFollowerIDs gets the follower IDs of screenNameOrID. cursorID specifies
//the cursor position for multiple request. Please refer to Twitter's API documentation on
//cursoring for more details.
func (n *Network) GetFollowerIDs(screenNameOrID interface{}, cursorID int64) ([]int64, int64) {
	return n.getUserIDs(screenNameOrID, "followers/ids", cursorID)
}

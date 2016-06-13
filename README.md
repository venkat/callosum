Callosum
============

[![GoDoc](https://godoc.org/github.com/venkat/callosum?status.svg)](https://godoc.org/github.com/venkat/callosum)

Callosum is a go package that helps build a corpus or collection from Twitter by recursively getting Twitter users and their tweets, friends, and followers. 

Callosum builds this corpus by storing the user and tweet information in a sqlite database. Callosum uses the [kuruvi](https://github.com/venkat/kuruvi) Twitter API client, which respects Twitter's API rate limits.

Callosum exposes a library of methods through the `TweetCollector` object, which you can use to control how you want to build your corpus. Start building your corpus by adding a set of Twitter user handles by calling the `SeedScreenNames` method. Then, call the `StartCollection` method to begin collecting their related users and tweets. The `FilterUser` function, a parameter for setting up the `TweetCollector`, is used to look at the user JSON response to decide which users to continue collection with.

Callosum can be stopped and restarted anytime, and it will pick up from where it left off.

### Example ###

This program collects the tweets of any user who might be interested in Etsy.

```go
package main

import (
    "encoding/json"
    "log"
    "strings"
    "time"

    "github.com/venkat/callosum"
)

//checkEtsyReference checks if the user's JSON response
//mentions "etsy" in their profile or in their latest tweet.
//If so, this user will be queued for further processing,
//which means their tweets, friends, and followers will be collected.
func checkEtsyReference(blob []byte) bool {
    var u *callosum.User
    err := json.Unmarshal(blob, &u)
    if err != nil {
        log.Fatal(err)
    }

    return strings.Contains(strings.ToLower(u.Description+u.LatestTweet.Text), "etsy")
}

func main() {
    log.SetFlags(log.Lshortfile)

    //auth.json contains the authentication tokens you get from Twitter's
    //Application Management portal - apps.twitter.com
    //Checkout template_auth.json for the format
    authFileName := "auth.json"

    //Twitter has a 15 minute rate limit rollover window.
    //Adding an extra minute to avoid edge cases with twitter
    //rolling over its time window
    window := 15*time.Minute + 1*time.Minute

    //Setting up the TwitterCollector
    //
    //"etsy" will be the name of the sqlite database file which will store
    //all the users and tweets data.
    t := callosum.NewTwitterCollector("etsy", authFileName, window, checkEtsyReference)

    //Starting out the collection by seeding a list of twitter users who refer to Etsy
    //in their profiles.
    t.SeedScreenNames([]string{"annacoder"})

    //This collects the tweets of the users in the given seed list and
    //collects the list of their friends and followers who also refer to Etsy (
    //and their friends and followers who also refer to Etsy and so on).
    t.StartCollection()
}
```

###TODO###
1. Optimize the sqlite file setup so that inserting and querying the table does not become dog slow when it has millions of users and tweets.
2. Batch insert user ids, users, and tweets in a transaction for better performance.
3. Write tests.
4. Add more examples.
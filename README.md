# Tracemap Backend

## Monitoring

- [[Netcup] Elastic Cluster](https://monitoring.tracemap.info/app/monitoring)
- [[Netcup] twitterLoginNode](https://monitoring.tracemap.info/app/apm#/tracemap-twitterLoginNode/transactions?_g=())
- [[Netcup] tracemap-api](https://monitoring.tracemap.info/app/apm#/tracemap-backend-api/transactions?_g=())
- [[Netcup] Netcup host](https://monitoring.tracemap.info/app/kibana#/dashboard/79ffd6e0-faa0-11e6-947f-177f697178b8?_g=())
- [[Netcup] Netcup traffic flow](https://monitoring.tracemap.info/app/kibana#/dashboard/Packetbeat-Flows?_g=())
- [[Netcup] Netcup nginx](https://monitoring.tracemap.info/app/kibana#/dashboard/55a9e6e0-a29e-11e7-928f-5dbe6f6f5519?_g=())
- [HTTP, TCP, ICMP Heartbeats](https://monitoring.tracemap.info/app/kibana#/dashboard/3406ee80-deb0-11e8-807c-ed3005088bdb?_g=(refreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-15m%2Cmode%3Aquick%2Cto%3Anow)))


## How to run the project

First you will need to configure your environment variables, create a file named `.env` on the root folder of the project with this content:

```
NEO4J_URI=bolt://neo4j:7687/db/data/
NEO4J_USER=neo4j
NEO4J_PASSWORD=root
APP_TOKEN=<YOUR TWITTER APP TOKEN>
APP_SECRET=<YOUR TWITTER APP SECRET>
USER_TOKEN=<YOUR USER TOKEN>
USER_SECRET=<YOUR USER SECRET>
```

In order to get your twitter tokens you have to create a new twitter app at https://apps.twitter.com/app/new, this is very quick to do. You will also need to create a new user token and fill the fields on the .env file.

With the tokens set up, run tracemap docker container with this command:

```
docker-compose up --build
```

You can check that the connection with twitter is working by executing:

```
curl "http://localhost:5000/twitter/get_tweet_data/947121935690944512" -v
```

And you can check that the connection with neo4j is working by executing:

```
curl "http://localhost:5000/neo4j/get_user_info/870716233393614849" -v
```

You can find all other API endpoints on the `server.py` file.

## Crawling

In order to populate your database for some users, you will need to call this endpoint:

```
curl "http://localhost:5000/neo4j/label_unknown_users/870716233393614849" -v
```
or with using multiple ids:

```
curl "http://localhost:5000/neo4j/label_unknown_users/870716233393614849,24618749124147,124217641123" -v
```

This will queue the user to be crawled, then for building the twitter followers graph you will need to run the crawler, you can do that by adding your twitter tokens to the database and then running the crawler queue.

```
docker-compose exec web make add-token
docker-compose exec web make run-crawler
```

Then you can check the user was actually crawled with:

```
curl "http://localhost:5000/neo4j/get_user_info/870716233393614849" -v
```

## Debugging

If you need debugging, you can switch to the web or database containers with:

```
docker-compose exec web bash
docker-compose exec neo4j bash
```

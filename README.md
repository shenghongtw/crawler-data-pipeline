# slack-airflow-selenium

Airflow + selenium crwaling vocus post and sending daily to my Slack.


## Set up

TODO: 
1.download docker
2.set up slack token in"\data\vocus_crawler\credentials"
3.run command "docker compose -f dockercompose.yml up" on this folder



Specify these secrets in the `Dockerfile`:

```
# Secrets
ENV weather_api_key=
ENV slack_webhook_url=
```

Run these commands in:

```
docker build . -t airflow
docker-compose -f docker-compose.yml up -d
```

The web server will be launched at `localhost:8080`

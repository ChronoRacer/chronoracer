# Dashboard

For documentation, see the [main document](../docs/component-dashboard.md).

To run locally (assuming InfluxDB Docker Compose from [here](../core/dev-only/dev-docker/influx_db/docker-compose.yaml) is also running):

```pwsh
docker run --rm `
  --name chronoracer-dashboard `                                                                                                                                                                                                 
  --add-host host.docker.internal:host-gateway `                                                                                                                                                                                 
  -p 3000:3000 `                                                                                                                                                                                                                 
  -e GF_SERVER_HTTP_PORT=3000 `                                                                                                                                                                                                  
  -e GF_SERVER_ROOT_URL=http://localhost:3000/ `                                                                                                                                                                                 
  -e GF_AUTH_GENERIC_OAUTH_CLIENT_ID="QwcTbS7a6Tn70WZo58UIl57mDxaJZp0V" `                                                                                                                                                        
  -e GF_AUTH_GENERIC_OAUTH_CLIENT_SECRET="$CLIENT_SECRET" `                                                                                                                                                                      
  -e GF_AUTH_GENERIC_OAUTH_AUTH_URL="https://chronoracer.eu.auth0.com/authorize" `                                                                                                                                               
  -e GF_AUTH_GENERIC_OAUTH_TOKEN_URL="https://chronoracer.eu.auth0.com/oauth/token" `                                                                                                                                            
  -e GF_AUTH_GENERIC_OAUTH_API_URL="https://chronoracer.eu.auth0.com/userinfo" `                                                                                                                                                 
  -e INFLUX_URL="http://host.docker.internal:8181" `                                                                                                                                                                             
  -e INFLUX_DATABASE="chrono_db" `                                                                                                                                                                                               
  -e INFLUX_TOKEN="$token" `                                                                                                                                                                                                     
  europe-west1-docker.pkg.dev/chronoracer/chronoracer/dashboard:latest   
```
Our Okta app has `localhost:3000` added as valid callback URLs.
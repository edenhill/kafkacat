export KAFKACAT_HOME="/app/.heroku"

# write kafka ssl config vars to files
mkdir -p $KAFKACAT_HOME/env

echo "$KAFKA_TRUSTED_CERT" > $KAFKACAT_HOME/env/KAFKA_TRUSTED_CERT
echo "$KAFKA_CLIENT_CERT" > $KAFKACAT_HOME/env/KAFKA_CLIENT_CERT
echo "$KAFKA_CLIENT_CERT_KEY" > $KAFKACAT_HOME/env/KAFKA_CLIENT_CERT_KEY

#
# Attempt at dynamically parsing through all kafka URLS and assigning them to different env variables 
#
# COUNT=1
# echo $KAFKA_URL | sed -n 1'p' | tr ',' '\n' | while read word; do
#     var=KAFKA_URL_$COUNT
#     export $var="$word"
#     (( COUNT++ ))
# done

# parse kafka url config vars into env var
IFS=', ' read -r -a array <<< "$KAFKA_URL"
export KAFKA_URL_1=`echo ${array[0]} | cut -d'/' -f3`

export PATH="/app/bin:$PATH"
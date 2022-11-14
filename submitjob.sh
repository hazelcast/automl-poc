#!/bin/zsh

docker compose run hazelcast-shell /opt/hazelcast/bin/hz-cli submit -v  -c=com.hzsamples.automl.PredictionPipeline \
  -t=hazelcast:5701  \
  /opt/project/scoring-pipeline/target/scoring-pipeline-1.0-SNAPSHOT.jar hazelcast-33 us-central1 4731246912831750144 

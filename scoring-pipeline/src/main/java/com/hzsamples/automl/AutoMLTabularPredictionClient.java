package com.hzsamples.automl;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.EndpointName;
import com.google.cloud.aiplatform.v1.PredictResponse;
import com.google.cloud.aiplatform.v1.PredictionServiceClient;
import com.google.cloud.aiplatform.v1.PredictionServiceSettings;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static com.hzsamples.automl.PredictionPipeline.stringValue;

public class AutoMLTabularPredictionClient  {
    private final PredictionServiceClient predictionServiceClient;
    private final EndpointName endpointName;

    public AutoMLTabularPredictionClient(String project, String location, String endpointId) throws IOException {
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("/Users/rmay/Downloads/hazelcast-33-9f491b43f63b.json"))
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        PredictionServiceSettings predictionServiceSettings =
                PredictionServiceSettings.newBuilder().setEndpoint(location + "-aiplatform.googleapis.com:443")
                        .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
        predictionServiceClient = PredictionServiceClient.create(predictionServiceSettings);
        endpointName = EndpointName.of(project, location, endpointId);
    }

    public PredictResponseExtractor predict(Struct features){
        Value featureVal = Value.newBuilder().setStructValue(features).build();
        PredictResponse response =  predictionServiceClient.predict(
                endpointName,
                Collections.singletonList(featureVal),
                Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build());

        return new PredictResponseExtractor(response);
    }

    /*
     * The program below shows example usage of the AutoMLTabularPredictionClient
     */
    public static void main(String []args){
        String project = "hazelcast-33";
        String location = "us-central1";
        String endpointId = "4731246912831750144";

        Struct features = Struct.newBuilder()
                .putFields("gender", stringValue("F"))
                .putFields("city", stringValue("Gainesville"))
                .putFields("state", stringValue("FL"))
                .putFields("lat", stringValue("38"))
                .putFields("long", stringValue("-87"))
                .putFields("city_pop", stringValue("62283"))
                .putFields("job", stringValue("officer"))
                .putFields("dob", stringValue("1978-12-14"))
                .putFields("category", stringValue("grocery_pos"))
                .putFields("amt", stringValue("25000"))
                .putFields("merchant", stringValue("Guido"))
                .putFields("merch_lat", stringValue("38"))
                .putFields("merch_long", stringValue("-87")).build();

        try {
            AutoMLTabularPredictionClient client = new AutoMLTabularPredictionClient(project, location, endpointId);
            AutoMLTabularPredictionClient.PredictResponseExtractor response = client.predict(features);
            System.out.println("Fraud Score: " + response.getPrediction(0,"1"));
        } catch (IOException iox){
            iox.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public static class PredictResponseExtractor implements Serializable {
        PredictResponse response;

        public PredictResponseExtractor(PredictResponse response){
            this.response = response;
        }

        public int getPredictionCount(){
            return response.getPredictionsCount();
        }

        public double getPrediction(int predictionNumber, String classId){
            // I don't know whether the lists inside a single prediction are
            // always in the same order so caching a map of class name to
            // index might not be safe
            List<Value> classes = response.getPredictions(predictionNumber).getStructValue()
                    .getFieldsOrThrow("classes").getListValue().getValuesList();
            int i=0;
            while(i < classes.size()){
                if (classId.equals(classes.get(i).getStringValue())) break;
                ++i;
            }
            if (i == classes.size())
                throw new RuntimeException("No class named \"" + classId + "\" was present in the result");

            return response.getPredictions(predictionNumber).getStructValue().getFieldsOrThrow("scores")
                    .getListValue().getValues(i).getNumberValue();
        }
    }

}

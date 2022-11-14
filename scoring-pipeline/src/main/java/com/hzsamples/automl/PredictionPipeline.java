package com.hzsamples.automl;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.*;
//import net.sourceforge.argparse4j.ArgumentParsers;
//import net.sourceforge.argparse4j.inf.ArgumentParser;
//import net.sourceforge.argparse4j.inf.ArgumentParserException;
//import net.sourceforge.argparse4j.inf.Namespace;

import java.util.Map;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class PredictionPipeline {
    public static Pipeline buildPipeline(String modelProject, String modelLocation, String modelEndpointId, float fraudConfidenceThreshold){
        Pipeline result = Pipeline.create();

        ServiceFactory<?, AutoMLTabularPredictionClient> predictionService =
                ServiceFactories.nonSharedService(c -> new AutoMLTabularPredictionClient(
                        modelProject,
                        modelLocation,
                        modelEndpointId)).toNonCooperative();

        StreamStage<Map.Entry<String, byte[]>> serializedAuthRequests =
                result.readFrom(Sources.<String, byte[]>mapJournal("auth_requests", JournalInitialPosition.START_FROM_OLDEST))
                .withIngestionTimestamps().setName("Input");

        StreamStage<AuthRequestv01> authRequests =
                serializedAuthRequests.map(entry -> AuthRequestv01.parseFrom(entry.getValue()))
                        .setName("deserialize Proto");

        // should I just put this functionality into the client ?
        StreamStage<Tuple2<AuthRequestv01, Struct>> authReqProtos =
                authRequests.map(authReq -> tuple2(authReq, authRequestToFeature(authReq)))
                        .setName("map to predict api features");

        StreamStage<Tuple2<AuthRequestv01, AutoMLTabularPredictionClient.PredictResponseExtractor>> predictions
                = authReqProtos.mapUsingService(predictionService, (ps, tuple) -> tuple2(tuple.f0(), ps.predict(tuple.f1())))
                .setName("call predict api");

        StreamStage<Tuple2<AuthRequestv01, Boolean>> decisions =
                predictions.map((tuple) -> tuple2(tuple.f0(), tuple.f1().getPrediction(0, "1") > fraudConfidenceThreshold))
                        .setName("classify");

        decisions.writeTo(Sinks.logger((t) -> (t.f1() ? "DECLINED " : "APPROVED" ) + t.f0().getAmt() + " on " + t.f0().getCategory() + " in " + t.f0().getCity() + ", " + t.f0().getState()));
        return result;
    }

    public  static Value stringValue(String v){
        return Value.newBuilder().setStringValue(v).build();
    }

    public static Struct authRequestToFeature(AuthRequestv01 authReq){
        return Struct.newBuilder()
                .putFields("gender", stringValue(authReq.getGender()))
                .putFields("city", stringValue(authReq.getCity()))
                .putFields("state", stringValue(authReq.getState()))
                .putFields("lat", stringValue(authReq.getLat()))
                .putFields("long", stringValue(authReq.getLong()))
                .putFields("city_pop", stringValue(authReq.getCityPop()))
                .putFields("job", stringValue(authReq.getJob()))
                .putFields("dob", stringValue(authReq.getDob()))
                .putFields("category", stringValue(authReq.getCategory()))
                .putFields("amt", stringValue(authReq.getAmt()))
                .putFields("merchant", stringValue(authReq.getMerchant()))
                .putFields("merch_lat", stringValue(authReq.getMerchLat()))
                .putFields("merch_long", stringValue(authReq.getMerchLong())).build();
    }

    public static void main(String []args){
//        ArgumentParser parser = ArgumentParsers.newFor("PredictionPipeline").build()
//                .description("Deploy the credit fraud prediction pipeline to Hazelcast").defaultHelp(true);
//        parser.addArgument("--gcloud-project").required(true).type(String.class).help("the Google Cloud project that hosts the fraud model");
//        parser.addArgument("--gcloud-region").required(true).type(String.class).help("the Google Cloud region containing the endpoint for the fraud model");
//        parser.addArgument("--gcloud-model-endpoint-id").required(true).type(String.class).help("The endpoint id where the fraud model is hosted");
//        parser.addArgument("--fraud-decision-threshold").type(Float.class).setDefault(Float.valueOf("0.5")).help("The prediction threshold above which a transaction will be classified as fraudulent");
//
//        Namespace ns = null;
//        try {
//            ns = parser.parseArgs(args);
//        } catch(ArgumentParserException x){
//            parser.handleError(x);
//            System.exit(1);
//        }
//        String project = ns.getString("gcloud_project");
//        String region = ns.getString("gcloud_region");
//        String endpointId = ns.getString("gcloud_model_endpoint_id");
//        float fraudDecisionThreshold = ns.getFloat("fraud_decision_threshold");

        if (args.length < 3){
            System.err.println("Requires at least 3 arguments: project region endpoint-id [fraud-threshold]");
        }

        String project = args[0];
        String region = args[1];
        String endpointId = args[2];
        float fraudDecisionThreshold = 0.5f;

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Auth Request Pipeline");
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(PredictionPipeline.buildPipeline(project,region,endpointId, fraudDecisionThreshold));
    }
}

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.transcribe.AmazonTranscribe;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.*;


public class S3EventListenerLambda implements RequestHandler<S3EventNotification,String> {

    public String handleRequest(S3EventNotification s3EventNotification, Context context) {

        String objectName = s3EventNotification.getRecords().get(0).getS3().getObject().getKey();
        String bucketName = s3EventNotification.getRecords().get(0).getS3().getBucket().getName();
        AmazonTranscribe client = AmazonTranscribeClient.builder().build();
        Media media = new Media().withMediaFileUri(generateURI("eu-central-1",bucketName,objectName));
        Settings transcriptSettings = new Settings();
        transcriptSettings.setChannelIdentification(false);
        transcriptSettings.setShowSpeakerLabels(false);
        StartTranscriptionJobRequest transcriptionJobRequest =
                new StartTranscriptionJobRequest()
                .withTranscriptionJobName(objectName.split("/")[1])
                .withMedia(media)
                .withMediaFormat(MediaFormat.Wav)
                .withLanguageCode(LanguageCode.EnUS)
                .withSettings(transcriptSettings)
                .withMediaSampleRateHertz(8000)
                .withOutputBucketName("results-transcribe-radio");
        client.startTranscriptionJob(transcriptionJobRequest);

        return "OK";
    }

    private String generateURI(String region,String bucketName,String objectName){
        //https://s3.eu-central-1.amazonaws.com/transcribe-wav-files/t
        return "https://s3."+region+".amazonaws.com/"+bucketName+"/"+objectName;
    }
}

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.transcribe.AmazonTranscribe;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.impl.client.HttpClients;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;

public class Main {
    public static final String BUCKET_NAME = "tgs-transcribe";
    public static final String FILE_NAME_IN_BUCKET = "to-process.mp3";
    public static final String AUDIO_FILE = "D:\\test-file.mp3";

    public static final String TRANS_JOB_NAME = "tgs-java-client-job";

    private static AmazonS3 s3 = AmazonS3ClientBuilder
            .standard()
            .withRegion(Regions.EU_WEST_1)
            .withClientConfiguration(new ClientConfiguration())
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .build();

    private static AmazonTranscribe client = AmazonTranscribeClient
            .builder()
            .withRegion(Regions.EU_WEST_1)
            .build();

    public static void main(String[] args) throws IOException, InterruptedException {
        long jobUid = ZonedDateTime.now().toInstant().toEpochMilli();
        String currentJobName = TRANS_JOB_NAME + "-" + jobUid;

        s3.putObject(BUCKET_NAME, FILE_NAME_IN_BUCKET, new File(AUDIO_FILE));

        StartTranscriptionJobRequest request = new StartTranscriptionJobRequest();
        request.withLanguageCode(LanguageCode.EnUS);

        Media media = new Media();
        media.setMediaFileUri(s3.getUrl(BUCKET_NAME, FILE_NAME_IN_BUCKET).toString());
        request.withMedia(media);

        request.setTranscriptionJobName(currentJobName);
        request.withMediaFormat("mp3");

        client.startTranscriptionJob(request);

        boolean keepProcessing = true;
        while (keepProcessing) {
            GetTranscriptionJobRequest getTranscriptionJobRequest = new GetTranscriptionJobRequest().withTranscriptionJobName(currentJobName);
            GetTranscriptionJobResult getTranscriptionJobResult = client.getTranscriptionJob(getTranscriptionJobRequest);

            TranscriptionJob transcriptionJob = getTranscriptionJobResult.getTranscriptionJob();
            boolean completed = transcriptionJob.getTranscriptionJobStatus().equals(TranscriptionJobStatus.COMPLETED.name());
            if (completed) {
                String uri = transcriptionJob.getTranscript().getTranscriptFileUri();

                ObjectMapper mapper = new ObjectMapper();

                try (CloseableHttpClient client = HttpClients.createDefault()) {

                    HttpGet httpGet = new HttpGet(uri);

                    Object response = client.execute(httpGet, httpResponse ->
                            mapper.readValue(httpResponse.getEntity().getContent(), Object.class));

                    System.out.println(response);
                } finally {
                    keepProcessing = false;
                }
            } else {
                System.out.println("Processing...");
                Thread.sleep(5000);
            }
        }
    }

}

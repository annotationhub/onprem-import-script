using System.Net.Http.Json;
using CsvHelper;
using System.Globalization;
using System.Linq;
using Newtonsoft.Json;

namespace ImportAnnoLabSources
{

    class Program
    {
        static readonly HttpClient client = new HttpClient();
        static readonly string projectName = "Test Project";
        static readonly string groupName = "luke";
        static readonly string apiBaseUri = "https://annolab-api.vectorstech.com/v1";
        static readonly string uploadUri = $"{apiBaseUri}/source/upload-pdf";
        static readonly string pendingSourceUri = $"{apiBaseUri}/pending-source";

        static readonly string predictUri = $"{apiBaseUri}/model/infer/batch";
        static readonly string authHeader = "Api-Key 9MBR1PN-RRRMRTZ-H3N0ZWP-44V8EP8";

        static readonly int pollInterval = 15000;

        static readonly int[] modelGroup1 = new int[]{ 1 };
        static readonly int[] modelGroup2 = new int[]{ 2 };

        static async Task Main()
        {
          client.DefaultRequestHeaders.Add("Authorization", authHeader);

          var uploads = readFileList();
          var pendingSources = new List<PendingSource>();

          foreach (CSVImportRow row in uploads)
          {
            var result = await ImportDocument(row);
            pendingSources.Add(result.pendingSource);
            Console.WriteLine("Successfully uploaded <{0}>", result.pendingSource.name);
          }

          Console.WriteLine("Waiting on OCR to Complete...");
          var finishedSources = await PollUntilOcrComplete(pendingSources);
          Console.WriteLine("OCR Completed. Preparing to run predictions.");

          var jobs1 = await SubmitBatchInferences(finishedSources, modelGroup1);
          await PollUntilBatchJobsComplete(jobs1);
          Console.WriteLine("Batch 1 predictions Completed. Submitting second batch...");

          var jobs2 = await SubmitBatchInferences(finishedSources, modelGroup2);
          await PollUntilBatchJobsComplete(jobs2);
          Console.WriteLine("Batch 2 predictions Completed.");
        }

        static async Task<List<InferenceJob>> SubmitBatchInferences(List<PendingSource> pendingSources, int[] modelIds)
        {
          var inferenceStatuses = new List<InferenceJob>();

          foreach (int modelId in modelIds)
          {
            inferenceStatuses.Add(await SubmitBatchInference(pendingSources, modelId));
          }

          return inferenceStatuses;
        }

        static async Task<InferenceJob> SubmitBatchInference(List<PendingSource> pendingSources, int modelId)
        {
          var sourceIds = pendingSources
            .Where(ps => ps.finalSourceId != null)
            .Select(ps => ps.finalSourceId)
            .OfType<int>()
            .ToArray();
          var body = new SubmitInferenceBody() {
            groupName = groupName,
            projectIdentifier = projectName,
            outputLayerIdentifier = "Gold Set",
            modelIdentifier = modelId,
            sourceIds = sourceIds
          };

          var jsonData = JsonConvert.SerializeObject(body);
          var contentData = new StringContent(jsonData, System.Text.Encoding.UTF8, "application/json");
          var response = await client.PostAsync(predictUri, contentData);

          try
          {
            response.EnsureSuccessStatusCode();
            var status = await response.Content.ReadFromJsonAsync<InferenceJob>();

            return status;
          }
          catch (HttpRequestException e)
          {
            Console.WriteLine("Failed to submit batch for model id <{0}>. Error: {1} ", modelId, e.Message);
            throw e;
          }
        }

        static async Task<List<InferenceJob>> PollUntilBatchJobsComplete(List<InferenceJob> inferenceJobs)
        {
          var updatedList = new List<InferenceJob>();

          foreach (InferenceJob job in inferenceJobs)
          {
            if (job.status != "Finished" || job.status != "Errored" || job.status != "Complete") {
              var updatedJob = await client.GetFromJsonAsync<InferenceJob>($"{predictUri}/{job.inferenceJobId}");
              updatedList.Add(updatedJob);
            } else {
              updatedList.Add(job);
            }
          }

          var incompleteJob = updatedList
            .Find(job => job.status != "Finished" && job.status != "Complete" && job.status != "Errored");

          if (incompleteJob == null) {
            return updatedList;
          }

          Thread.Sleep(pollInterval);
          Console.WriteLine("Predictions Incomplete...polling");
          return await PollUntilBatchJobsComplete(updatedList);
        }

        static async Task<List<PendingSource>> PollUntilOcrComplete(List<PendingSource> pendingSources)
        {
          var updatedList = new List<PendingSource>();

          foreach (PendingSource ps in pendingSources)
          {
            if (ps.finalSourceId == null) {
              var updatedPs = await client.GetFromJsonAsync<PendingSource>($"{pendingSourceUri}/{ps.pendingSourceId}");
              updatedList.Add(updatedPs);

              if (updatedPs.finalSourceId != null) {
                Console.WriteLine("OCR complete for <{0}>", updatedPs.name);
              }
            } else {
              updatedList.Add(ps);
            }
          }

          var incompleteSource = updatedList
            .Find(ps => ps.status != "Complete" && ps.status != "Errored");

          if (incompleteSource == null) {
            return updatedList;
          }

          Thread.Sleep(pollInterval);
          return await PollUntilOcrComplete(updatedList);
        }

        static List<CSVImportRow> readFileList()
        {
          using (var reader = new StreamReader("/Users/lukesimkins/Projects/crossfold/aic-scripts/test.csv"))
          using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
          {
            return csv.GetRecords<CSVImportRow>().ToList();
          }
        }

        static async Task<UploadPdfResponse> ImportDocument(CSVImportRow row) {
          var fileStreamContent = new StreamContent(File.OpenRead(row.filepath));
          fileStreamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/pdf");
          var httpClientHandler = new HttpClientHandler() { UseDefaultCredentials = true };
          var httpClient = new HttpClient(httpClientHandler, true);
          var requestMessage = new HttpRequestMessage(HttpMethod.Post, uploadUri);
          var metadata = new DocumentMetadata() {
            orderId = row.orderId,
            orderItemId = row.orderItemId,
            make = row.make,
            model = row.model,
            serial = row.serial,
            nNumber = row.nNumber
          };
          var formDataContent = new MultipartFormDataContent() {
            { new StringContent(projectName), "projectIdentifier" },
            { new StringContent(groupName), "groupName" },
            { new StringContent(Path.GetFileName(row.filepath)), "sourceIdentifier" },
          };

          var jsonMetadata = new StringContent(
            JsonConvert.SerializeObject(metadata),
            System.Text.Encoding.UTF8,
            "application/json"
          );
          formDataContent.Add(jsonMetadata, "metadata");
          formDataContent.Add(fileStreamContent, name: "file", fileName: "source.pdf");
          requestMessage.Content = formDataContent;

          var response = await client.PostAsync(uploadUri, formDataContent);

          try
          {
            response.EnsureSuccessStatusCode();
            var ps = await response.Content.ReadFromJsonAsync<UploadPdfResponse>();

            return ps;
          }
          catch (HttpRequestException e)
          {
            Console.WriteLine("Failed Upload <{0}>. Error: {1} ", row.filepath, e.Message);
            throw e;
          }

        }
    }


    class CSVImportRow
    {
      public string filepath { get; set; }
      public string orderId { get; set; }
      public string orderItemId { get; set; }
      public string make { get; set; }
      public string model { get; set; }
      public string serial { get; set; }
      public string nNumber { get; set; }
    }

    class DocumentMetadata
    {
      public string orderId { get; set; }
      public string orderItemId { get; set; }
      public string make { get; set; }
      public string model { get; set; }
      public string serial { get; set; }
      public string nNumber { get; set; }
    }

    class UploadPdfResponse
    {
      public string? projectName { get; set; }
      public int? projectId { get; set; }

      public string? directoryName { get; set; }
      public int? directoryId { get; set; }
      public string? sourceName { get; set; }

      public PendingSource pendingSource { get; set; }
    }

    class PendingSource
    {
      public int? pendingSourceId { get; set; }
      public string? name { get; set; }
      public string? sourceType { get; set; }
      public string? createdAt { get; set; }
      public string? expiresAt { get; set; }
      public string? confirmedAt { get; set; }

      public string? status { get; set; }
      public int? finalSourceId { get; set; }
    }

    class InferenceJob
    {
      public int inferenceJobId { get; set; }
      public int projectId { get; set; }
      public int[] sourceIds { get; set; }
      
      public string status { get; set; }
    }

    class SubmitInferenceBody
    {
      public string groupName { get; set; }
      public string projectIdentifier { get; set; }
      public string outputLayerIdentifier { get; set; }
      public int modelIdentifier { get; set; }
      public int[] sourceIds { get; set; }
    }
}

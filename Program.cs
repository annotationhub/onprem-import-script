using System;
using System.Net;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using CsvHelper;
using System.Globalization;

namespace ImportAnnoLabSources
{

    class Program
    {
        static readonly HttpClient client = new HttpClient();
        static readonly string projectName = "AIC";
        static readonly string groupName = "lsimkins";
        static readonly string apiBaseUri = "http://localhost:8080/v1";
        static readonly string uploadUri = $"{apiBaseUri}/source/upload-pdf";
        static readonly string pendingSourceUri = $"{apiBaseUri}/pending-source";
        static readonly string authHeader = "Api-Key KS7YXWC-R5NM63T-H0NR63N-DVZBR86";

        static readonly int pollInterval = 15000;

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
          await PollUntilOcrComplete(pendingSources);
          Console.WriteLine("OCR Completed. Preparing to run predictions.");

        }

        static async Task<bool> PollUntilOcrComplete(List<PendingSource> pendingSources)
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

          var complete = true;
          foreach (PendingSource ps in updatedList)
          {
            Console.WriteLine("Pending Source <{0}> <{1}>", ps.finalSourceId, ps.status);
            if (ps.finalSourceId == null && ps.status != "Errored") {
              complete = false;
              break;
            }
          }

          if (!complete) {
            Thread.Sleep(pollInterval);
            return await PollUntilOcrComplete(updatedList);
          } else {
            return true;
          }

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
          var formDataContent = new MultipartFormDataContent() {
            { new StringContent(projectName), "projectIdentifier" },
            { new StringContent(groupName), "groupName" },
            { new StringContent(Path.GetFileName(row.filepath)), "sourceIdentifier" },
          };

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
}

using System;
using System.Net;
using System.IO;
using System.Net.Http;
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
        static readonly string authHeader = "Api-Key KS7YXWC-R5NM63T-H0NR63N-DVZBR86";


        static async Task Main()
        {
          client.DefaultRequestHeaders.Add("Authorization", authHeader);

          var uploads = readFileList();

          foreach (UploadDataRow row in uploads)
          {
            await ImportDocument(row);
          }
        }

        static IEnumerable<UploadDataRow> readFileList()
        {
          using (var reader = new StreamReader("/Users/lukesimkins/Projects/crossfold/aic-scripts/test.csv"))
          using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
          {
            return csv.GetRecords<UploadDataRow>().ToList();
          }
        }

        static async Task ImportDocument(UploadDataRow row) {
          var ms = new MemoryStream();
          using (FileStream file = new FileStream(row.filepath, FileMode.Open, FileAccess.Read)) {
            file.CopyTo(ms);
          }
          var fileStreamContent = new StreamContent(ms);
          fileStreamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/pdf");
          var httpClientHandler = new HttpClientHandler() { UseDefaultCredentials = true };
          var httpClient = new HttpClient(httpClientHandler, true);
          var requestMessage = new HttpRequestMessage(HttpMethod.Post, uploadUri);
          var formDataContent = new MultipartFormDataContent() {
            { new StringContent(projectName), "projectIdentifier" },
            { new StringContent(groupName), "groupName" },
            { new StringContent(Path.GetFileName(row.filepath)), "sourceIdentifier" },
            { fileStreamContent, "file", "source.pdf" }
          };

          requestMessage.Content = formDataContent;

          var response = await client.PostAsync(uploadUri, formDataContent);

          try
          {
            response.EnsureSuccessStatusCode();
          }
          catch (HttpRequestException e)
          {
            Console.WriteLine("Failed Upload :{0}. Error: :{1} ", row.filepath, e.Message);
          }

        }
    }


    class UploadDataRow
    {
      public string filepath { get; set; }
      public string orderItem { get; set; }
      public string orderItemId { get; set; }
      public string make { get; set; }
      public string model { get; set; }
      public string serial { get; set; }
      public string nNumber { get; set; }
    }
}
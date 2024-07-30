// <copyright file="Program.cs" company="PlaceholderCompany">
// Copyright (c) PlaceholderCompany. All rights reserved.
// </copyright>

namespace BatchProcessing
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Net.Http.Json;
    using System.Text.Json;
    using System.Threading.Tasks;
    using System.Globalization;
    using CsvHelper;

    public static class Program
    {
        public static async Task Main()
        {
            ListIds listContents = new ListContents();

            // process csv in batch args(localfile path, batch size, line to start reading from)
            await listContents.ProcessCsvInBatches("C:\\Users\\Varun\\Downloads\\fileToRead.csv", 1, 0);
        }
    }

  public class ListContents
  {
      readonly PushMessage _pushMessage;
  
      public ListIds()
      {
          PushMessage pushMessage = new PushMessage();
  
          this._pushMessage = pushMessage;
      }
  
      public async Task ProcessCsvInBatches(string filePath, int batchSize, int startLine)
      {
          using (var reader = new StreamReader(filePath))
          using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
          {
              int rowCount = 0;
  
              List<int> batch = new List<int>();
  
              // Skip lines until the start line
              while (rowCount < startLine - 1 && await csv.ReadAsync())
              {
                  rowCount++;
              }
  
              // Start processing from the start line
              while (await csv.ReadAsync())
              {
                  for (int i = 0; csv.TryGetField<int>(i, out int field); i++)
                  {
                      batch.Add(field);
                  }
  
                  rowCount++;
  
                  if ((rowCount - (startLine - 1)) % batchSize == 0)
                  {
                      try
                      {
                          await this._pushMessage.PushMessageToUrl(batch);
                          Console.WriteLine("message sent till row: " + rowCount.ToString());
                          Thread.Sleep(1000);
                          batch.Clear();
                      }
                      catch (Exception ex)
                      {
                          Console.WriteLine("batch processing failed for batch with " + rowCount.ToString());
                          Console.WriteLine(ex.ToString());
                      }
                  }
              }
  
              // Process the remaining rows in the last batch
              if (batch.Count > 0)
              {
                  try
                  {
                      await this._pushMessage.PushMessageToUrl(batch);
                      Console.WriteLine("message sent till row: " + rowCount.ToString());
                  }
                  catch (Exception ex)
                  {
                      Console.WriteLine("batch processing failed for batch with " + rowCount.ToString());
                      Console.WriteLine(ex.ToString());
                  }
              }
          }
      }
  }

  public class PushMessage
  {
      public async Task PushMessageToUrl(List<int> ids)
      {
          var client = new HttpClient();
          var url = "https://localhost:5000/resend";  // Replace with your actual API endpoint
  
          HttpResponseMessage response = await client.PostAsJsonAsync(url, ids);
  
          if (response.IsSuccessStatusCode)
          {
              Console.WriteLine("Request successful.");
          }
          else
          {
              Console.WriteLine($"Request failed with status code: {response.StatusCode}");
          }
      }
  }
}

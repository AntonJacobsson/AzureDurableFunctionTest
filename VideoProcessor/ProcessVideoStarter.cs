using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace VideoProcessor
{
    public static class ProcessVideoStarter
    {
        [FunctionName("ProcessVideoStarter")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]
            HttpRequestMessage req, [OrchestrationClient] DurableOrchestrationClient starter ,TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request ");

            string video = req.GetQueryNameValuePairs().FirstOrDefault(q => string.Compare(q.Key, "video", true) == 0).Value;

            dynamic data = await req.Content.ReadAsAsync<object>();
            video = video ?? data?.video;

            if( video == null)
            {
                return req.CreateResponse(HttpStatusCode.BadRequest, "not good");
            }
            log.Info($"About to start orchestation for {video}");

            var orchestrationId = await starter.StartNewAsync("O_ProcessVideo", video);
            return starter.CreateCheckStatusResponse(req, orchestrationId);

        }
        [FunctionName("SubmitVideoApproval")]
        public static async Task<HttpResponseMessage> SubmitVideoApproval(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "SubmitVideoApproval/{id}")]
            HttpRequestMessage req,
            [OrchestrationClient] DurableOrchestrationClient client,
            [Table("Approvals", "Approval", "{id}", Connection = "AzureWebJobsStorage")] Approval approval,
            TraceWriter log )
        {
            string result = req.GetQueryNameValuePairs().FirstOrDefault(q => string.Compare(q.Key, "result", true) == 0).Value;
            if (result == null)
            {
                return req.CreateResponse(HttpStatusCode.BadRequest, "not good");
            }
            await client.RaiseEventAsync(approval.OrchestrationId, "ApprovalResult", result);
            return req.CreateResponse(HttpStatusCode.OK);
        }
        [FunctionName("StartPeriodicTask")]
        public static async Task<HttpResponseMessage> StartPeriodicTask(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)]
         HttpRequestMessage req,
        [OrchestrationClient] DurableOrchestrationClient client,
        TraceWriter log)
        {
            var instanceId = await client.StartNewAsync("O_PeriodicTask", 0);
            return client.CreateCheckStatusResponse(req, instanceId);
        }

    }
}

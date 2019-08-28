using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessor
{
    public static class ProcessVideoOrchestrators
    {
        [FunctionName("O_ProcessVideo")]
        public static async Task<object> ProcessVideo([
            OrchestrationTrigger] DurableOrchestrationContext ctx, TraceWriter log)
        {
            var videoLocation = ctx.GetInput<string>();

            string transcodedLocation = null;
            string thumbnailLocation = null;
            string withIntroLocation = null;
            string approvalResult = "Unknown";

            try
            {
                var transcodeResults = await ctx.CallSubOrchestratorAsync<VideoFileInfo[]>("O_TranscodeVideo", videoLocation);

                transcodedLocation = transcodeResults.OrderByDescending(r => r.BitRate).Select(r => r.Location).First();

                if (!ctx.IsReplaying)
                {
                    log.Info("A_ExtractThumbnail Activity");
                }
                thumbnailLocation = await ctx.CallActivityAsync<string>("A_ExtractThumbnail", transcodedLocation);
                if (!ctx.IsReplaying)
                {
                    log.Info("A_PrependIntro Activity");
                }
                withIntroLocation = await ctx.CallActivityAsync<string>("A_PrependIntro", transcodedLocation);

                await ctx.CallActivityAsync("A_SendApprovalRequestEmail", new ApprovalInfo {
                    OrchestrationId = ctx.InstanceId,
                    VideoLocation = withIntroLocation
                });

                using (var cts = new CancellationTokenSource())
                {
                    var timeoutAt = ctx.CurrentUtcDateTime.AddSeconds(30);
                    var timeoutTask = ctx.CreateTimer(timeoutAt, cts.Token);
                    var approvalTask = ctx.WaitForExternalEvent<string>("ApprovalResult");

                    var winner = await Task.WhenAny(approvalTask, timeoutTask);
                    if (winner == approvalTask)
                    {
                        approvalResult = approvalTask.Result;
                    }
                    else
                    {
                        approvalResult = "Timed Out";
                    }
                }


                if (approvalResult == "Approved")
                {
                    await ctx.CallActivityAsync("A_PublishVideo", withIntroLocation);
                } else
                {
                    await ctx.CallActivityAsync("A_RejectVideo", withIntroLocation);
                }
            }
            catch (Exception e)
            {
                if (!ctx.IsReplaying)
                {
                    log.Info("caught an eerror from an activity" + e.Message);
                }
                await ctx.CallActivityAsync<string>("A_Cleanup", new[] { transcodedLocation, thumbnailLocation, withIntroLocation });
                return new
                {
                    Error = "Failed",
                    Message = e.Message
                };
            }

            return new
            {
                Transcoded = transcodedLocation,
                Thumbnail = thumbnailLocation,
                WithIntro = withIntroLocation,
                ApprovalResult = approvalResult,
            };
        }
        [FunctionName("O_TranscodeVideo")]
        public static async Task<VideoFileInfo[]> TranscodeVideo(
            [OrchestrationTrigger] DurableOrchestrationContext ctx, TraceWriter log)
        {
            var videoLocation = ctx.GetInput<string>();
            var bitRates = await ctx.CallActivityAsync<int[]>("A_GetTranscodeBitrates", null);
            var transcodeTasks = new List<Task<VideoFileInfo>>();

            foreach (var bitRate in bitRates)
            {
                var info = new VideoFileInfo() { Location = videoLocation, BitRate = bitRate };
                var task = ctx.CallActivityAsync<VideoFileInfo>("A_TranscodeVideo", info);
                transcodeTasks.Add(task);
            }

            var transcodeResults = await Task.WhenAll(transcodeTasks);
            return transcodeResults;
        }

        [FunctionName("O_PeriodicTask")]
        public static async Task<int> PeriodicTask(
           [OrchestrationTrigger] DurableOrchestrationContext ctx, TraceWriter log)
        {
            var timesRun = ctx.GetInput<int>();
            timesRun++;
            if (!ctx.IsReplaying)
            {
                log.Info("starting the periodic task activity " + ctx.InstanceId + " " + timesRun);
            }
            await ctx.CallActivityAsync("A_PeriodicActivity", timesRun);
            var nextRun = ctx.CurrentUtcDateTime.AddSeconds(15);
            await ctx.CreateTimer(nextRun, CancellationToken.None);
            ctx.ContinueAsNew(timesRun);
            return timesRun;
        }

    }
}


// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

//This class has a small modification made by Alex PG (agerman77@gmail.com)
//in the IssueWebRequestAsync method from the original one from Microsoft.
//The modification is about setting a new parameter (removeParam) to the method which allows it to remove the '?' sign from the request
//Tested on HBase Server ver 2.3.1

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.HBase.Client;
using Microsoft.HBase.Client.Internal.Helpers;
using Microsoft.HBase.Client.LoadBalancing;
using Microsoft.HBase.Client.Requester;
using agerman77.HBase.Client.Internal.Helpers;

namespace agerman77.HBase.Client.Requester
{
    public class HBaseWebRequester : IWebRequester
    {
        private readonly ILoadBalancer _balancer;
        private readonly string _contentType;
        private readonly CredentialCache _credentialCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseWebRequester"/> class.
        /// </summary>
        /// <param name="balancer">the load balancer for the vnet nodes</param>
        /// <param name="contentType">Type of the content.</param>
        public HBaseWebRequester(ILoadBalancer balancer, string contentType = "application/x-protobuf")
        {
            _balancer = balancer;
            _contentType = contentType;
            _credentialCache = null;
        }

        /// <summary>
        /// Issues the web request.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public Response IssueWebRequest(string endpoint, string query, string method, Stream input, RequestOptions options)
        {
            return IssueWebRequest(endpoint, query, method, input, options, false);
        }

        /// <summary>
        /// Issues the web request.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public Response IssueWebRequest(string endpoint, string query, string method, Stream input, RequestOptions options, bool? removeQuestionMark)
        {
            return IssueWebRequestAsync(endpoint, query, method, input, options, removeQuestionMark).Result;
        }


        public async Task<Response> IssueWebRequestAsync(string endpoint, string query, string method, Stream input, RequestOptions options)
        {
            return await IssueWebRequestAsync(endpoint, query, method, input, options, false);
        }

        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public async Task<Response> IssueWebRequestAsync(string endpoint, string query, string method, Stream input, RequestOptions options, bool? removeQuestionMark)
        {
            options.Validate();
            Stopwatch watch = Stopwatch.StartNew();
            Trace.CorrelationManager.ActivityId = Guid.NewGuid();
            var balancedEndpoint = _balancer.GetEndpoint();

            // Grab the host. Use the alternative host if one is specified
            string host = (options.AlternativeHost != null) ? options.AlternativeHost : balancedEndpoint.Host;

            UriBuilder builder = new UriBuilder(
                balancedEndpoint.Scheme,
                host,
                options.Port,
                options.AlternativeEndpoint + endpoint);

            if (query != null)
            {
                builder.Query = query;
            }

            var target = builder.Uri;
            if (removeQuestionMark == true)
            {
                target = new Uri(builder.Uri.ToString().Replace("/?", "/"));
            }


            try
            {
                Debug.WriteLine("Issuing request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

                HttpWebRequest httpWebRequest = WebRequest.CreateHttp(target);
                //httpWebRequest.ServicePoint.ReceiveBufferSize = options.ReceiveBufferSize;
                httpWebRequest.ServicePoint.UseNagleAlgorithm = options.UseNagle;
                httpWebRequest.Timeout = options.TimeoutMillis; // This has no influence for calls that are made Async
                httpWebRequest.KeepAlive = options.KeepAlive;
                httpWebRequest.Credentials = _credentialCache;
                httpWebRequest.PreAuthenticate = true;
                httpWebRequest.Method = method;
                httpWebRequest.Accept = _contentType;
                httpWebRequest.ContentType = _contentType;
                // This allows 304 (NotModified) requests to catch
                //https://msdn.microsoft.com/en-us/library/system.net.httpwebrequest.allowautoredirect(v=vs.110).aspx
                httpWebRequest.AllowAutoRedirect = false;

                if (options.AdditionalHeaders != null)
                {
                    foreach (var kv in options.AdditionalHeaders)
                    {
                        httpWebRequest.Headers.Add(kv.Key, kv.Value);
                    }
                }
                long remainingTime = options.TimeoutMillis;

                if (input != null)
                {
                    // expecting the caller to seek to the beginning or to the location where it needs to be copied from
                    Stream req = null;
                    try
                    {
                        req = await httpWebRequest.GetRequestStreamAsync().WithTimeout(
                                                    TimeSpan.FromMilliseconds(remainingTime),
                                                    "Waiting for RequestStream");

                        remainingTime = options.TimeoutMillis - watch.ElapsedMilliseconds;
                        if (remainingTime <= 0)
                        {
                            remainingTime = 0;
                        }

                        await input.CopyToAsync(req).WithTimeout(
                                    TimeSpan.FromMilliseconds(remainingTime),
                                    "Waiting for CopyToAsync",
                                    CancellationToken.None);
                    }
                    catch (TimeoutException)
                    {
                        httpWebRequest.Abort();
                        throw;
                    }
                    finally
                    {
                        if (req != null)
                        {
                            req.Close();
                        }
                    }
                }

                try
                {
                    remainingTime = options.TimeoutMillis - watch.ElapsedMilliseconds;
                    if (remainingTime <= 0)
                    {
                        remainingTime = 0;
                    }

                    Debug.WriteLine("Waiting for response for request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

                    HttpWebResponse response = (HttpWebResponse)await httpWebRequest.GetResponseAsync().WithTimeout(
                                                                    TimeSpan.FromMilliseconds(remainingTime),
                                                                    "Waiting for GetResponseAsync");

                    Debug.WriteLine("Web request {0} to endpoint {1} successful!", Trace.CorrelationManager.ActivityId, target);

                    return new Response()
                    {
                        WebResponse = response,
                        RequestLatency = watch.Elapsed,
                        PostRequestAction = (r) =>
                        {
                            if (r.WebResponse.StatusCode == HttpStatusCode.OK || r.WebResponse.StatusCode == HttpStatusCode.Created || r.WebResponse.StatusCode == HttpStatusCode.NotModified)
                            {
                                _balancer.RecordSuccess(balancedEndpoint);
                            }
                            else
                            {
                                _balancer.RecordFailure(balancedEndpoint);
                            }
                        }
                    };
                }
                catch (TimeoutException)
                {
                    httpWebRequest.Abort();
                    throw;
                }
            }
            catch (WebException we)
            {
                // 404 is valid response
                var resp = we.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    _balancer.RecordSuccess(balancedEndpoint);
                    Debug.WriteLine("Web request {0} to endpoint {1} successful!", Trace.CorrelationManager.ActivityId, target);
                }
                else
                {
                    _balancer.RecordFailure(balancedEndpoint);
                    Debug.WriteLine("Web request {0} to endpoint {1} failed!", Trace.CorrelationManager.ActivityId, target);
                }
                throw we;
            }
            catch (Exception e)
            {
                _balancer.RecordFailure(balancedEndpoint);
                Debug.WriteLine("Web request {0} to endpoint {1} failed!", Trace.CorrelationManager.ActivityId, target);
                throw e;
            }
        }
    }
}

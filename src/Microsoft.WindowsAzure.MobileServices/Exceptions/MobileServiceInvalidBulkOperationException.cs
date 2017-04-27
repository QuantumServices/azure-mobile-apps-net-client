using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;

namespace Microsoft.WindowsAzure.MobileServices
{
    /// <summary>
    /// Provides additional details of an invalid operation specific to a
    /// Mobile Service.
    /// </summary>
    public class MobileServiceInvalidBulkOperationException : InvalidOperationException
    {
        // <summary>
        /// Initializes a new instance of the
        /// MobileServiceInvalidBulkOperationException class.
        /// </summary>
        /// <param name="message">
        /// The exception message.
        /// </param>
        /// <param name="request">
        /// The originating service request.
        /// </param>
        /// <param name="response">
        /// The returned service response.
        /// </param>
        public MobileServiceInvalidBulkOperationException(string message, HttpRequestMessage request, HttpResponseMessage response)
            : this(message, request, response, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the
        /// MobileServiceInvalidBulkOperationException class.
        /// </summary>
        /// <param name="message">
        /// The exception message.
        /// </param>
        /// <param name="request">
        /// The originating service request.
        /// </param>
        /// <param name="response">
        /// The returned service response.
        /// </param>
        /// <param name="value">
        /// Server response deserialized as JObject.
        /// </param>
        public MobileServiceInvalidBulkOperationException(string message, HttpRequestMessage request, HttpResponseMessage response, JArray values)
            : base(message)
        {
            this.Request = request;
            this.Response = response;
            this.Values = values;
        }

        /// <summary>
        /// Gets the originating service request.
        /// </summary>
        public HttpRequestMessage Request { get; private set; }

        /// <summary>
        /// Gets the returned service response.
        /// </summary>
        public HttpResponseMessage Response { get; private set; }

        /// <summary>
        /// Server response deserialized as JArray.
        /// </summary>
        public JArray Values { get; private set; }
    }
}
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices
{
    /// <summary>
    /// Provides details of http response with status code of 'Conflict' for a bulk operation
    /// </summary>
    public class MobileServiceBulkConflictException : MobileServiceInvalidBulkOperationException
    {
        /// <summary>
        /// Initializes a new instance of the
        /// <see cref="MobileServiceBulkConflictException"/> class.
        /// </summary>
        /// <param name="source">
        /// The inner exception.
        /// </param>
        /// <param name="value">
        /// The current instance from the server that the conflict occurred for.
        /// </param>
        public MobileServiceBulkConflictException(MobileServiceInvalidBulkOperationException source, JArray values)
            : base(source.Message, source.Request, source.Response, values)
        {
        }
    }

    /// <summary>
    /// Provides details of http response with status code of 'Conflict' for a bulk operation
    /// </summary>
    public class MobileServiceBulkConflictException<T> : MobileServiceBulkConflictException
    {
        /// <summary>
        /// Initializes a new instance of the
        /// <see cref="MobileServiceBulkConflictException"/> class.
        /// </summary>
        /// <param name="source">
        /// The inner exception.
        /// </param>
        /// <param name="items">
        /// The current instances from the server that the conflict occurred for.
        /// </param>
        public MobileServiceBulkConflictException(MobileServiceInvalidBulkOperationException source, IEnumerable<T> items)
            : base(source, source.Values)
        {
            this.Items = items;
        }

        /// <summary>
        /// The current instances from the server that the precondition failed for.
        /// </summary>
        public IEnumerable<T> Items { get; private set; }
    }
}
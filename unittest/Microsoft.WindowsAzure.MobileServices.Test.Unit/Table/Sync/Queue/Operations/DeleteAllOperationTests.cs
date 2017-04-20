using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.MobileServices.Sync;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.Test.Unit.Table.Sync.Queue.Operations
{
    [TestClass]
    public class DeleteAllOperationTests
    {
        private DeleteAllOperation operation;
        private int operationCount = 1000;

        [TestInitialize]
        public void Initialize()
        {
            var itemIds = Enumerable.Range(0, operationCount).Select(i => $"abc{i}").ToList();
            this.operation = new DeleteAllOperation("test", MobileServiceTableKind.Table, itemIds);
        }

        [TestMethod]
        public async Task ExecuteLocalAsync_DeletesItemOnStore()
        {
            var store = new Mock<IMobileServiceLocalStore>();
            await this.operation.ExecuteLocalAsync(store.Object, null);
            store.Verify(s => s.DeleteAsync("test", operation.ItemIds), Times.Once());
        }
    }
}
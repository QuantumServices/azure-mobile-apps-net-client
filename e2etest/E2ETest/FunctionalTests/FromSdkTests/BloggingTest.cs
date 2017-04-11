﻿// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.MobileServices.TestFramework;
using Newtonsoft.Json;
using System.Linq;
using System;

namespace Microsoft.WindowsAzure.MobileServices.Test
{
    [DataTable("blog_posts")]
    public class BlogPost
    {
        public string Id { get; set; }

        [JsonProperty(PropertyName = "title")]
        public string Title { get; set; }

        [JsonProperty(PropertyName = "commentCount")]
        public int CommentCount { get; set; }
    }

    [DataTable("blog_comments")]
    public class BlogComment
    {
        public string Id { get; set; }

        [JsonProperty(PropertyName = "postid")]
        public string BlogPostId { get; set; }

        [JsonProperty(PropertyName = "name")]
        public string UserName { get; set; }

        [JsonProperty(PropertyName = "commentText")]
        public string Text { get; set; }
    }

    [DataContract(Name = "blog_posts")]
    public class DataContractBlogPost
    {
        [DataMember]
        public string Id = null;

        [DataMember(Name = "title")]
        public string Title;

        [DataMember(Name = "commentCount")]
        public int CommentCount { get; set; }

        [DataMember]
        public byte[] Version { get; set; }
    }

    [Tag("Blog")]
    public class BloggingTest : FunctionalTestBase
    {
        [AsyncTestMethod]
        public async Task PostComments()
        {
            IMobileServiceClient client = GetClient();
            IMobileServiceTable<BlogPost> postTable = client.GetTable<BlogPost>();
            IMobileServiceTable<BlogComment> commentTable = client.GetTable<BlogComment>();
            var userDefinedParameters = new Dictionary<string, string>() { { "state", "NY" }, { "tags", "#pizza #beer" } };

            // Add a few posts and a comment
            Log("Adding posts");
            BlogPost post = new BlogPost { Title = "Windows 8" };
            await postTable.InsertAsync(post, userDefinedParameters);
            BlogPost highlight = new BlogPost { Title = "ZUMO" };
            await postTable.InsertAsync(highlight);
            await commentTable.InsertAsync(new BlogComment
            {
                BlogPostId = post.Id,
                UserName = "Anonymous",
                Text = "Beta runs great"
            });
            await commentTable.InsertAsync(new BlogComment
            {
                BlogPostId = highlight.Id,
                UserName = "Anonymous",
                Text = "Whooooo"
            });
            Assert.AreEqual(2, (await postTable.Where(p => p.Id == post.Id || p.Id == highlight.Id)
                                                .WithParameters(userDefinedParameters)
                                                .ToListAsync()).Count);

            // Navigate to the first post and add a comment
            Log("Adding comment to first post");
            BlogPost first = await postTable.LookupAsync(post.Id);
            Assert.AreEqual("Windows 8", first.Title);
            BlogComment opinion = new BlogComment { BlogPostId = first.Id, Text = "Can't wait" };
            await commentTable.InsertAsync(opinion);
            Assert.IsFalse(string.IsNullOrWhiteSpace(opinion.Id));
        }

        [AsyncTestMethod]
        public async Task BulkPostComments()
        {
            IMobileServiceClient client = GetClient();
            IMobileServiceTable<BlogPost> postTable = client.GetTable<BlogPost>();
            IMobileServiceTable<BlogComment> commentTable = client.GetTable<BlogComment>();

            // Add a few posts and a comment
            Log("Adding post");
            BlogPost post = new BlogPost { Title = "Windows 8" };
            await postTable.InsertAsync(post);
            IList<BlogComment> comments = new List<BlogComment>();

            for (int i = 0; i < 5000; i++)
            {
                comments.Add(new BlogComment
                {
                    BlogPostId = post.Id,
                    UserName = "Anonymous",
                    Text = $"Beta runs great {i}"
                });
            }

            await commentTable.InsertAsync(comments);
            Assert.IsFalse(comments.Any(c => string.IsNullOrWhiteSpace(c.Id)));
        }

        [AsyncTestMethod]
        public async Task BulkUpdateComments()
        {
            IMobileServiceClient client = GetClient();
            IMobileServiceTable<BlogPost> postTable = client.GetTable<BlogPost>();
            IMobileServiceTable<BlogComment> commentTable = client.GetTable<BlogComment>();

            // Add a few posts and a comment
            Log("Adding post");
            BlogPost post = new BlogPost { Title = "Windows 8" };
            await postTable.InsertAsync(post);
            IList<BlogComment> comments = new List<BlogComment>();

            for (int i = 0; i < 5000; i++)
            {
                comments.Add(new BlogComment
                {
                    BlogPostId = post.Id,
                    UserName = "Anonymous",
                    Text = $"Beta runs great {i}"
                });
            }

            await commentTable.InsertAsync(comments);
            Assert.IsFalse(comments.Any(c => string.IsNullOrWhiteSpace(c.Id)));

            foreach (var comment in comments)
            {
                comment.Text = comment.Text + "1";
            }

            await commentTable.UpdateAsync(comments);
        }

        [AsyncTestMethod]
        public async Task PostExceptionMessageFromZumoRuntime()
        {
            IMobileServiceClient client = GetClient();
            IMobileServiceTable<BlogPost> postTable = client.GetTable<BlogPost>();

            // Delete a bog post that doesn't exist to get an exception generated by the
            // runtime.
            try
            {
                await postTable.DeleteAsync(new BlogPost() { CommentCount = 5, Id = "this_does_not_exist" });
            }
            catch (MobileServiceInvalidOperationException e)
            {
                bool isValid = e.Message.Contains("The request could not be completed.  (Not Found)") ||
                    e.Message.Contains("The item does not exist");
                Assert.IsTrue(isValid, "Unexpected error message: " + e.Message);
            }
        }

        [AsyncTestMethod]
        public async Task PostExceptionMessageFromUserScript()
        {
            IMobileServiceClient client = GetClient();
            IMobileServiceTable<BlogPost> postTable = client.GetTable<BlogPost>();

            // Insert a blog post that doesn't have a title; the user script will respond
            // with a 400 and an error message string in the response body.
            try
            {
                await postTable.InsertAsync(new BlogPost() { });
            }
            catch (MobileServiceInvalidOperationException e)
            {
                Assert.AreEqual("All blog posts must have a title.", e.Message);
            }
        }

        [AsyncTestMethod]
        public async Task PostCommentsWithDataContract()
        {
            IMobileServiceClient client = GetClient();
            IMobileServiceTable<DataContractBlogPost> postTable = client.GetTable<DataContractBlogPost>();

            // Add a few posts and a comment
            Log("Adding posts");
            DataContractBlogPost post = new DataContractBlogPost() { Title = "How DataContracts Work" };
            await postTable.InsertAsync(post);
            DataContractBlogPost highlight = new DataContractBlogPost { Title = "Using the 'DataMember' attribute" };
            await postTable.InsertAsync(highlight);

            Assert.AreEqual(2, (await postTable.Where(p => p.Id == post.Id || p.Id == highlight.Id).ToListAsync()).Count);
        }
    }
}
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Threading.Tasks;

namespace AWS.Wrapper.S3
{
    /// <summary>
    /// opration class for AWS S3.
    /// </summary>
    public class AWSS3Client : IDisposable
    {
        /// <summary>
        /// AmazonS3Client instance
        /// </summary>
        private AmazonS3Client _client;

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="accessKeyId"></param>
        /// <param name="secretAccessKey"></param>
        /// <param name="regionEndpoint"></param>
        /// <param name="timeout"></param>
        public AWSS3Client(string accessKeyId, string secretAccessKey, Amazon.RegionEndpoint regionEndpoint, double timeout = 600d)
        {
            var config = new AmazonS3Config
            {
                RegionEndpoint = regionEndpoint,
                Timeout = TimeSpan.FromSeconds(timeout),
            };
            _client = new AmazonS3Client(accessKeyId, secretAccessKey, config);
        }

        /// <summary>
        /// dispose
        /// </summary>
        public void Dispose()
        {
            _client.Dispose();
        }

        /// <summary>
        /// get expire(by DateTime.Now)
        /// </summary>
        /// <param name="expireMinites"></param>
        /// <returns></returns>
        private DateTime GetExpire(double expireMinites)
        {
            return DateTime.Now.AddMinutes(expireMinites);
        }

        /// <summary>
        /// get signed url
        /// </summary>
        /// <param name="backetName"></param>
        /// <param name="key"></param>
        /// <param name="verb"></param>
        /// <param name="expireMinites"></param>
        /// <returns></returns>
        public string GetPreSignedUrl(string backetName, string key, HttpVerb verb, double expireMinites)
        {
            var request = new GetPreSignedUrlRequest
            {
                BucketName = backetName,
                Verb = verb,
                Key = key,
                Expires = GetExpire(expireMinites),
            };

            return _client.GetPreSignedURL(request);
        }

        /// <summary>
        /// get objects by async
        /// </summary>
        /// <param name="backetName"></param>
        /// <param name="key"></param>
        /// <param name="expireMinites"></param>
        /// <returns></returns>
        public async Task<GetObjectResponse> GetObjectAsync(string backetName, string key, double expireMinites)
        {
            var request = new GetObjectRequest
            {
                BucketName = backetName,
                Key = key,
                ResponseExpiresUtc = GetExpire(expireMinites),
            };

            return await _client.GetObjectAsync(request);
        }

        /// <summary>
        /// put objects by async
        /// </summary>
        /// <param name="backetName"></param>
        /// <param name="key"></param>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public async Task<PutObjectResponse> PutObjectAsync(string backetName, string key, string filePath)
        {
            var request = new PutObjectRequest
            {
                BucketName = backetName,
                Key = key,
                FilePath = filePath,
            };
            return await _client.PutObjectAsync(request);
        }

        private ListObjectsV2Request _listObjectsV2Request { get; set; }
        private ListObjectsV2Request ListObjectsV2Request
        {
            get
            {
                _listObjectsV2Request = _listObjectsV2Request ?? new ListObjectsV2Request();
                return _listObjectsV2Request;
            }
        }

        /// <summary>
        /// listObjects by by async
        /// </summary>
        /// <param name="backetName"></param>
        /// <param name="prefix"></param>
        /// <param name="continuationToken"></param>
        /// <returns></returns>
        public async Task<ListObjectsV2Response> ListObjectsV2Async(string backetName, string prefix, string continuationToken = null)
        {

            ListObjectsV2Request.BucketName = backetName;
            ListObjectsV2Request.Prefix = prefix;
            ListObjectsV2Request.ContinuationToken = continuationToken;

            return await _client.ListObjectsV2Async(ListObjectsV2Request);
        }
    }
}

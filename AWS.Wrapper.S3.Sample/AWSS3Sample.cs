using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AWS.Wrapper.S3.Sample
{
    /// <summary>
    /// sample class for aws s3 client
    /// </summary>
    public class AWSS3Sample
    {
        /// <summary>
        /// access key id
        /// </summary>
        private readonly string _accessKeyId = "";

        /// <summary>
        /// secret access key
        /// </summary>
        private readonly string _secretAccessKey = "";

        /// <summary>
        /// region
        /// </summary>
        private readonly RegionEndpoint _region = RegionEndpoint.APNortheast1;

        /// <summary>
        /// backet name
        /// </summary>
        private readonly string _backetName = "";

        /// <summary>
        /// expire(minites)
        /// </summary>
        private readonly double _expireMinites = 5d;

        #region method

        /// <summary>
        /// get signed download url
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string GetSignedDownloadUrl(string key)
        {
            using (var client = new AWSS3Client(_accessKeyId, _secretAccessKey, _region))
            {
                return client.GetPreSignedUrl(_backetName, key, HttpVerb.GET, _expireMinites);
            }
        }

        /// <summary>
        /// get signed upload url
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string GetSignedUploadUrl(string key)
        {
            using (var client = new AWSS3Client(_accessKeyId, _secretAccessKey, _region))
            {
                return client.GetPreSignedUrl(_backetName, key, HttpVerb.PUT, _expireMinites);
            }
        }

        /// <summary>
        /// download object
        /// </summary>
        /// <param name="key"></param>
        /// <param name="filePath"></param>
        /// <param name="appendFile"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task DownloadObjectAsync(string key, string filePath, bool appendFile, CancellationToken cancellationToken)
        {
            using (var client = new AWSS3Client(_accessKeyId, _secretAccessKey, _region))
            {
                using (var result = await client.GetObjectAsync(_backetName, key, _expireMinites))
                {
                    await result.WriteResponseStreamToFileAsync(filePath, appendFile, cancellationToken);
                }
            }
        }

        /// <summary>
        /// download object
        /// </summary>
        /// <param name="key"></param>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public async Task<PutObjectResponse> UploadObjectAsync(string key, string filePath)
        {
            using (var client = new AWSS3Client(_accessKeyId, _secretAccessKey, _region))
            {
                 return await client.PutObjectAsync(_backetName, key, filePath);
            }
        }

        /// <summary>
        /// get object keys
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetObjectKeysAsync(string prefix)
        {
            var objectKeys = new List<string>();

            using (var client = new AWSS3Client(_accessKeyId, _secretAccessKey, _region))
            {
                string continuationToken = null;
                var isTruncated = true;
                while (isTruncated)
                {
                    var result = await client.ListObjectsV2Async(_backetName, prefix, continuationToken);
                    if (result.S3Objects != null && result.S3Objects.Any())
                    {
                        objectKeys.AddRange(result.S3Objects.Select(s3Object => s3Object.Key).ToArray());
                    }
                    isTruncated = result.IsTruncated;
                    continuationToken = result.NextContinuationToken;
                }
            }
            return objectKeys;
        }

        #endregion
    }
}

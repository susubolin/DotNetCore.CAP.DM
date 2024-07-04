// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Data.SqlTypes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dm;
using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotNetCore.CAP.DM
{
    public class DMStorageInitializer : IStorageInitializer
    {
        private readonly IOptions<DMOptions> _options;
        private readonly ILogger _logger;

        public DMStorageInitializer(
            ILogger<DMStorageInitializer> logger,
            IOptions<DMOptions> options)
        {
            _options = options;
            _logger = logger;
        }

        public virtual string GetPublishedTableName()
        {
            return $"{_options.Value.TableNamePrefix}.published";
        }

        public virtual string GetReceivedTableName()
        {
            return $"{_options.Value.TableNamePrefix}.received";
        }

        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested) return;

            var sql = CreateDbTablesScript();
            var sqlarr=sql.Split(";");
            foreach(var executesql in sqlarr)
            {
                if (!string.IsNullOrEmpty(executesql))
                {
                    await using (var connection = new DmConnection(_options.Value.ConnectionString))
                        connection.ExecuteNonQuery(executesql);
                }
            
            }

            
            
            _logger.LogDebug("Ensuring all create database tables script are applied.");
        }


        protected virtual string CreateDbTablesScript()
        {
            //""Group"" VARCHAR(200) NULL,

               var batchSql = $@"CREATE TABLE IF NOT EXISTS ""{GetReceivedTableName()}"" (
                                ""Id"" BIGINT NOT NULL PRIMARY KEY,
                                ""Version"" VARCHAR(20),
                                ""Name"" VARCHAR(200) NOT NULL,
                                ""Content"" CLOB,
                                ""Retries"" INT,
                                ""Added"" TIMESTAMP(0) NOT NULL,
                                ""ExpiresAt"" TIMESTAMP(0),
                                ""StatusName"" VARCHAR(40) NOT NULL);

                                CREATE OR REPLACE  INDEX ""IX_Received_ExpiresAt"" ON ""{GetReceivedTableName()}""(""ExpiresAt"");

                                CREATE TABLE IF NOT EXISTS ""{GetPublishedTableName()}"" (
                                ""Id"" BIGINT NOT NULL PRIMARY KEY,
                                ""Version"" VARCHAR(20),
                                ""Name"" VARCHAR(200) NOT NULL,
                                ""Content"" CLOB,
                                ""Retries"" INT,
                                ""Added"" TIMESTAMP(0) NOT NULL,
                                ""ExpiresAt"" TIMESTAMP(0),
                                ""StatusName"" VARCHAR(40) NOT NULL);

                                CREATE OR REPLACE  INDEX ""IX_Published_ExpiresAt"" ON ""{GetPublishedTableName()}""(""ExpiresAt"");";

            return batchSql;
        }
    }
}
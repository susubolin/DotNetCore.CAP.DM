﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dm;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.Options;

namespace DotNetCore.CAP.DM
{
    internal class DMMonitoringApi : IMonitoringApi
    {
        private readonly DMOptions _options;
        private readonly string _pubName;
        private readonly string _recName;

        public DMMonitoringApi(IOptions<DMOptions> options, IStorageInitializer initializer)
        {
            _options = options.Value ?? throw new ArgumentNullException(nameof(options));
            _pubName = initializer.GetPublishedTableName();
            _recName = initializer.GetReceivedTableName();
        }

        public StatisticsDto GetStatisticsAsync()
        {
            var sql = $@"
    SELECT
    (
        SELECT COUNT(""Id"") FROM {_pubName} WHERE ""StatusName"" = N'Succeeded'
    ) AS ""PublishedSucceeded"",
    (
        SELECT COUNT(""Id"") FROM {_recName} WHERE ""StatusName"" = N'Succeeded'
    ) AS ""ReceivedSucceeded"",
    (
        SELECT COUNT(""Id"") FROM {_pubName} WHERE ""StatusName"" = N'Failed'
    ) AS ""PublishedFailed"",
    (
        SELECT COUNT(""Id"") FROM {_recName} WHERE ""StatusName"" = N'Failed'
    ) AS ""ReceivedFailed"";";
            StatisticsDto statistics;
            using (var connection = new DmConnection(_options.ConnectionString))
            {
                 statistics = connection.ExecuteReader(sql, reader =>
                {
                    var statisticsDto = new StatisticsDto();

                    while (reader.Read())
                    {
                        statisticsDto.PublishedSucceeded = reader.GetInt32(0);
                        statisticsDto.ReceivedSucceeded = reader.GetInt32(1);
                        statisticsDto.PublishedFailed = reader.GetInt32(2);
                        statisticsDto.ReceivedFailed = reader.GetInt32(3);
                    }

                    return statisticsDto;
                });
            }
            return statistics;
        }

        public IDictionary<DateTime, int> HourlyFailedJobs(MessageType type)
        {
            var tableName = type == MessageType.Publish ? _pubName : _recName;
            return GetHourlyTimelineStats(tableName, nameof(StatusName.Failed));
        }

        public IDictionary<DateTime, int> HourlySucceededJobs(MessageType type)
        {
            var tableName = type == MessageType.Publish ? _pubName : _recName;
            return GetHourlyTimelineStats(tableName, nameof(StatusName.Succeeded));
        }
        public PagedQueryResult<MessageDto> Messages(MessageQueryDto queryDto)
        {
            var tableName = queryDto.MessageType == MessageType.Publish ? _pubName : _recName;
            var where = string.Empty;
            if (!string.IsNullOrEmpty(queryDto.StatusName))
            {
                where += " and StatusName=:StatusName";
            }

            if (!string.IsNullOrEmpty(queryDto.Name))
            {
                where += " and Name=:Name";
            }

            //if (!string.IsNullOrEmpty(queryDto.Group))
            //{
            //    where += " and Group=:Group";
            //}

            if (!string.IsNullOrEmpty(queryDto.Content))
            {
                where += " and Content like CONCAT('%',:Content,'%')";
            }

            var sqlQuery =
                $"select * from \"{tableName}\" where 1=1 {where} order by Added desc offset :Offset limit :Limit";

            object[] sqlParams =
            {
                new DmParameter(":StatusName", queryDto.StatusName ?? string.Empty),
                //new DmParameter(":Group", queryDto.Group ?? string.Empty),
                new DmParameter(":Name", queryDto.Name ?? string.Empty),
                new DmParameter(":Content", $"%{queryDto.Content}%"),
                new DmParameter(":Offset", queryDto.CurrentPage * queryDto.PageSize),
                new DmParameter(":Limit", queryDto.PageSize)
            };

            using var connection = new DmConnection(_options.ConnectionString);

            var count = connection.ExecuteScalar<int>($"select count(1) from \"{tableName}\" where 1=1 {where}",
                new DmParameter(":StatusName", queryDto.StatusName ?? string.Empty),
                //new DmParameter(":Group", queryDto.Group ?? string.Empty),
                new DmParameter(":Name", queryDto.Name ?? string.Empty),
                new DmParameter(":Content", $"%{queryDto.Content}%"));

            var items=  connection.ExecuteReader(sqlQuery, reader =>
            {
                var messages = new List<MessageDto>();
                while (reader.Read())
                {
                    var index = 0;
                    messages.Add(new MessageDto
                    {
                        Id = reader.GetInt64(index++).ToString(),
                        Version = reader.GetString(index++),
                        Name = reader.GetString(index++),
                        //Group = queryDto.MessageType == MessageType.Subscribe ? reader.GetString(index++) : default,
                        Content = reader.GetString(index++),
                        Retries = reader.GetInt32(index++),
                        Added = reader.GetDateTime(index++),
                        ExpiresAt = reader.IsDBNull(index++) ? (DateTime?)null : reader.GetDateTime(index - 1),
                        StatusName = reader.GetString(index)
                    });
                }
                return messages;

            }, sqlParams);
            return new PagedQueryResult<MessageDto>
            { Items = items, PageIndex = queryDto.CurrentPage, PageSize = queryDto.PageSize, Totals = count };
        }

        public int PublishedFailedCount()
        {
            return GetNumberOfMessage(_pubName, nameof(StatusName.Failed));
        }

        public int PublishedSucceededCount()
        {
            return GetNumberOfMessage(_pubName, nameof(StatusName.Succeeded));
        }

        public int ReceivedFailedCount()
        {
            return GetNumberOfMessage(_recName, nameof(StatusName.Failed));
        }

        public int ReceivedSucceededCount()
        {
            return GetNumberOfMessage(_recName, nameof(StatusName.Succeeded));
        }

        private int GetNumberOfMessage(string tableName, string statusName)
        {
            var sqlQuery = $"select count(\"Id\") from {tableName} where Lower(\"StatusName\") = Lower(:state)";

            using var connection = new DmConnection(_options.ConnectionString);
            return connection.ExecuteScalar<int>(sqlQuery, new DmParameter(":state", statusName));
        }

        private Dictionary<DateTime, int> GetHourlyTimelineStats(string tableName, string statusName)
        {
            var endDate = DateTime.Now;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => x.ToString("yyyy-MM-dd-HH"), x => x);

            return GetTimelineStats(tableName, statusName, keyMaps);
        }

        private Dictionary<DateTime, int> GetTimelineStats(
            string tableName,
            string statusName,
            IDictionary<string, DateTime> keyMaps)
        {
            var sqlQuery = $@"
with aggr as (
    select to_char(""Added"",'yyyy-MM-dd-HH') as ""Key"",
    count(""Id"") as ""Count""
    from {tableName}
        where ""StatusName"" = :statusName
    group by to_char(""Added"", 'yyyy-MM-dd-HH')
)
select ""Key"",""Count"" from aggr where ""Key"" >= :minKey and ""Key"" <= :maxKey;";

            object[] sqlParams =
            {
                new DmParameter(":statusName", statusName),
                new DmParameter(":minKey", keyMaps.Keys.Min()),
                new DmParameter(":maxKey", keyMaps.Keys.Max())
            };

            Dictionary<string, int> valuesMap;
            using (var connection = new DmConnection(_options.ConnectionString))
            {
                valuesMap = connection.ExecuteReader(sqlQuery, reader =>
                {
                    var dictionary = new Dictionary<string, int>();

                    while (reader.Read())
                    {
                        dictionary.Add(reader.GetString(0), reader.GetInt32(1));
                    }

                    return dictionary;
                }, sqlParams);
            }

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key))
                {
                    valuesMap.Add(key, 0);
                }
            }

            var result = new Dictionary<DateTime, int>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }

        public async Task<MediumMessage?> GetPublishedMessageAsync(long id) => await GetMessageAsync(_pubName, id);

        public async Task<MediumMessage?> GetReceivedMessageAsync(long id) => await GetMessageAsync(_recName, id);

        private async Task<MediumMessage?> GetMessageAsync(string tableName, long id)
        {
            var sql = $@"SELECT ""Id"" AS ""DbId"", ""Content"", ""Added"", ""ExpiresAt"", ""Retries"" FROM {tableName} WHERE ""Id""={id} FOR UPDATE SKIP LOCKED";

            await using var connection = new DmConnection(_options.ConnectionString);
            var mediumMessage = connection.ExecuteReader(sql, reader =>
            {
                MediumMessage? message = null;

                while (reader.Read())
                {
                    message = new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Content = reader.GetString(1),
                        Added = reader.GetDateTime(2),
                        ExpiresAt = reader.GetDateTime(3),
                        Retries = reader.GetInt32(4)
                    };
                }

                return message;
            });

            return mediumMessage;
        }

        public StatisticsDto GetStatistics()
        {
            throw new NotImplementedException();
        }

     
    }
}

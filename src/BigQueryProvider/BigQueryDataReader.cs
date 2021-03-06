/*
   Copyright 2015-2018 Developer Express Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using DevExpress.DataAccess.BigQuery.Native;
using Google;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;

namespace DevExpress.DataAccess.BigQuery
{
    /// <summary>
    /// Provides access to resulting data of command execution.
    /// </summary>
    public class BigQueryDataReader : DbDataReader
    {
        #region Fields
        private bool disposed;
        private IEnumerator<TableRow> enumerator;
        private int fieldsCount;
        private IList<TableRow> rows;
        private TableSchema schema;
        #endregion

        #region Constants
        private const char parameterPrefix = '@';
        private readonly BigQueryCommand bigQueryCommand;
        private readonly BigqueryService bigQueryService;
        #endregion

        #region Properties
        private bool IsStandardSql => bigQueryCommand.Connection.IsStandardSql;

        /// <summary>
        /// Gets the nesting depth of the current data row. BigQuery does now support nesting, and this property always returns
        /// 0;
        /// </summary>
        /// <value>
        /// the nesting depth of the current data row.
        /// </value>
        public override int Depth => 0;

        /// <summary>
        /// Indicates whether the current BigQueryDataReader is closed.
        /// </summary>
        /// <value>
        /// true, if the current BigQueryDataReader is closed; otherwise false.
        /// </value>
        public override bool IsClosed => disposed;

        /// <summary>
        /// Returns the total count of data records affected by executing a command.
        /// </summary>
        /// <value>
        /// the number of rows affected.
        /// </value>
        public override int RecordsAffected => 0;

        /// <summary>
        /// Gets the number of visible columns in the current row.
        /// </summary>
        /// <value>
        /// The number of visible columns in the reader.
        /// </value>
        public override int VisibleFieldCount => FieldCount;

        /// <summary>
        /// Gets the total number of columns in the current row.
        /// </summary>
        /// <value>
        /// the number of columns in the current row.
        /// </value>
        public override int FieldCount
        {
            get
            {
                DisposeCheck();
                return fieldsCount;
            }
        }

        /// <summary>
        /// Gets the value of the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <returns>The value of the specified column.</returns>
        public override object this[int ordinal]
        {
            get
            {
                DisposeCheck();
                return GetValue(ordinal);
            }
        }

        /// <summary>
        /// Gets the value of a column specified by its name.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <returns>The value of the specified column.</returns>
        public override object this[string name]
        {
            get
            {
                DisposeCheck();
                var ordinal = GetOrdinal(name);
                return GetValue(ordinal);
            }
        }

        /// <summary>
        /// Indicates whether or not there are more rows in the reader.
        /// </summary>
        /// <value>
        /// true, if there are more rows in the reader; otherwise false.
        /// </value>
        public override bool HasRows
        {
            get
            {
                DisposeCheck();
                return rows != null && rows.Count > 0;
            }
        }
        #endregion

        #region Constructor
        internal BigQueryDataReader(BigQueryCommand command, BigqueryService service)
        {
            bigQueryService = service;
            bigQueryCommand = command;
        }
        #endregion

        /// <summary>
        /// Closes the current BigQueryDataReader.
        /// </summary>
        public override void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Returns the value of the specified column as a type.
        /// </summary>
        /// <typeparam name="T">The type of a value to return.</typeparam>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column. </returns>
        public override T GetFieldValue<T>(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<T>(value, ordinal);
        }

        /// <summary>
        /// Returns a data table containing column metadata of the reader.
        /// </summary>
        /// <returns>a DataTable containing column metadata.</returns>
        public override DataTable GetSchemaTable()
        {
            DisposeCheck();

            var dataTable = new DataTable();
            dataTable.Columns.Add("ColumnName", typeof(string));
            dataTable.Columns.Add("Description", typeof(string));
            dataTable.Columns.Add("DataType", typeof(Type));
            dataTable.Columns.Add("ETag", typeof(string));
            dataTable.Columns.Add("Mode", typeof(string));

            foreach (var field in schema.Fields)
            {
                dataTable.Rows.Add(
                    field.Name,
                    field.Description,
                    BigQueryTypeConverter.ToType(field.Type),
                    field.ETag,
                    field.Mode
                );
            }

            return dataTable;
        }

        /// <summary>
        /// Advances the reader to the next result set.
        /// </summary>
        /// <returns>true, if there are more result, sets; otherwise false.</returns>
        public override bool NextResult()
        {
            DisposeCheck();
            return false;
        }

        /// <summary>
        /// Advances the reader to the next record in the current result set.
        /// </summary>
        /// <returns>true, if there are more records in the result set; otherwise, false.</returns>
        public override bool Read()
        {
            DisposeCheck();
            return enumerator.MoveNext();
        }

        /// <summary>
        /// Gets a value of the System.Boolean type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override bool GetBoolean(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<bool>(value, ordinal);
        }

        /// <summary>
        /// Gets a value of the System.Byte type from the specified column. This implementation always throws
        /// NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override byte GetByte(int ordinal)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Reads bytes from the specified column into a buffer. This implementation always throws NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <param name="dataOffset">An offset in the source data from which to start reading.</param>
        /// <param name="buffer">A buffer to which to copy data. </param>
        /// <param name="bufferOffset">An offset in the buffer from which to start writing.</param>
        /// <param name="length">The maximum number of bytes to read.</param>
        /// <returns>The number of bytes that has been read.</returns>
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets a value of the System.Guid type from the specified column. This implementation always throws
        /// NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override char GetChar(int ordinal)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Reads characters from the specified column into a buffer. This implementation always throws NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <param name="dataOffset">An offset in the source data from which to start reading.</param>
        /// <param name="buffer">A buffer to which to copy data. </param>
        /// <param name="bufferOffset">An offset in the buffer from which to start writing.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <returns>The number of characters that has been read.</returns>
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets a value of the System.Guid type from the specified column. This implementation always throws
        /// NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override Guid GetGuid(int ordinal)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets a value of the System.Int16 type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override short GetInt16(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<short>(value, ordinal);
        }

        /// <summary>
        /// Gets a value of the System.Int32 type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override int GetInt32(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<int>(value, ordinal);
        }

        /// <summary>
        /// Gets a value of the System.Int64 type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override long GetInt64(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<long>(value, ordinal);
        }

        /// <summary>
        /// Gets a value of the System.DateTime type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override DateTime GetDateTime(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<DateTime>(value, ordinal);
        }

        /// <summary>
        /// Gets a value of the System.String type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override string GetString(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<string>(value, ordinal);
        }

        /// <summary>
        /// Gets a value of the System.Decimal type from the specified column. This implementation always throws
        /// NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override decimal GetDecimal(int ordinal)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets a value of the System.Double type from the specified column. This implementation always throws
        /// NotSupportedException.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override double GetDouble(int ordinal)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets a value of the float type from the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The value of the specified column.</returns>
        public override float GetFloat(int ordinal)
        {
            var value = GetValue(ordinal);
            return ChangeValueType<float>(value, ordinal);
        }

        /// <summary>
        /// Returns the name of the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <returns>A string containing the name of the specified column.</returns>
        public override string GetName(int ordinal)
        {
            DisposeCheck();
            RangeCheck(ordinal);
            return schema.Fields[ordinal].Name;
        }

        /// <summary>
        /// Fills an array with provider-specific values of all columns in the reader.
        /// </summary>
        /// <param name="values">An array to which to copy obtained values.</param>
        /// <returns>The number of values added to the array.</returns>
        public override int GetProviderSpecificValues(object[] values)
        {
            return GetValues(values);
        }

        /// <summary>
        /// Fills an array with values of all columns in the reader.
        /// </summary>
        /// <param name="values">An array to which to copy obtained values.</param>
        /// <returns>The number of values added to the array.</returns>
        public override int GetValues(object[] values)
        {
            DisposeCheck();

            for (var i = 0; i < fieldsCount; i++)
                values[i] = GetValue(i);

            return values.Length;
        }

        /// <summary>
        /// Returns a value that indicates whether the content of the specified column is equal to System.Data.DbNull.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <returns>true, if the content of the specified column is equal to System.Data.DbNull; otherwise, false.</returns>
        public override bool IsDBNull(int ordinal)
        {
            DisposeCheck();
            RangeCheck(ordinal);
            return GetValue(ordinal) == null;
        }

        /// <summary>
        /// Returns the ordinal number of a column specified by its name.
        /// </summary>
        /// <param name="name">the name of a column.</param>
        /// <returns>The ordinal number of the specified column.</returns>
        public override int GetOrdinal(string name)
        {
            DisposeCheck();

            for (var i = 0; i < schema.Fields.Count; i++)
                if (schema.Fields[i].Name == name)
                    return i;

            return -1;
        }

        /// <summary>
        /// Gets the name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of the required column.</param>
        /// <returns>The name of the specified column.</returns>
        public override string GetDataTypeName(int ordinal)
        {
            DisposeCheck();
            return GetFieldType(ordinal)?.Name;
        }

        /// <summary>
        /// Gets the provider-specific type of the specified column.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <returns>The type of the specified column.</returns>
        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            return GetFieldType(ordinal);
        }

        /// <summary>
        /// Gets the type of the specified column.
        /// </summary>
        /// <param name="ordinal"> The ordinal number of a column.</param>
        /// <returns>The type of the specified column.</returns>
        public override Type GetFieldType(int ordinal)
        {
            DisposeCheck();
            RangeCheck(ordinal);
            var type = schema.Fields[ordinal].Type;
            var fieldType = BigQueryTypeConverter.ToType(type);

            if (fieldType != null)
                return fieldType;

            throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, "No field with ordinal");
        }

        /// <summary>
        /// Returns the provider-specific value of the specified field.
        /// </summary>
        /// <param name="ordinal">The ordinal number of a column.</param>
        /// <returns>The value of the specified column.</returns>
        public override object GetProviderSpecificValue(int ordinal)
        {
            return GetValue(ordinal);
        }

        /// <summary>
        /// Gets a value of the Object type from the specified column.
        /// </summary>
        /// <param name="ordinal"> The ordinal number of a column.</param>
        /// <returns>The value of the specified column.</returns>
        public override object GetValue(int ordinal)
        {
            DisposeCheck();
            RangeCheck(ordinal);
            return ChangeValueType(enumerator.Current?.F[ordinal].V, ordinal);
        }

        /// <summary>
        /// returns an enumerator that allows iterating through all rows in the reader.
        /// </summary>
        /// <returns>an object implementing the IEnumerator interface.</returns>
        public override IEnumerator GetEnumerator()
        {
            DisposeCheck();
            return enumerator;
        }

        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                ((BigQueryParameterCollection)bigQueryCommand.Parameters).Validate();

                var queryResponse = await CreateRequest()
                    .ExecuteAsync(cancellationToken)
                    .ConfigureAwait(false);

                ProcessQueryResponse(queryResponse);
            }
            catch (GoogleApiException e)
            {
                throw e.Wrap();
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                enumerator?.Dispose();

            disposed = true;
            base.Dispose(disposing);
        }

        private JobsResource.QueryRequest CreateRequest()
        {
            var collection = (BigQueryParameterCollection)bigQueryCommand.Parameters;

            if (!IsStandardSql)
                foreach (BigQueryParameter parameter in collection)
                {
                    bigQueryCommand.CommandText = bigQueryCommand.CommandText.Replace(parameterPrefix + parameter.ParameterName.TrimStart(parameterPrefix),
                        ConvertToStringForLegacySql(parameter));
                }

            var queryRequest = new QueryRequest
            {
                Query = PrepareCommandText(bigQueryCommand),
                TimeoutMs = bigQueryCommand.CommandTimeout != 0
                    ? (int)TimeSpan.FromSeconds(bigQueryCommand.CommandTimeout).TotalMilliseconds
                    : int.MaxValue,
                UseLegacySql = !IsStandardSql
            };

            if (!string.IsNullOrEmpty(bigQueryCommand.Connection.DataSetId))
            {
                queryRequest.DefaultDataset = new DatasetReference
                {
                    DatasetId = bigQueryCommand.Connection.DataSetId,
                    ProjectId = bigQueryCommand.Connection.ProjectId
                };
            }

            if (IsStandardSql)
            {
                queryRequest.QueryParameters = new List<QueryParameter>();

                foreach (BigQueryParameter parameter in collection)
                {
                    var queryParameter = new QueryParameter
                    {
                        Name = parameter.ParameterName,
                        ParameterType = new QueryParameterType
                        {
                            Type = BigQueryTypeConverter.ToParameterStringType(parameter.BigQueryDbType)
                        },
                        ParameterValue = new QueryParameterValue { Value = ConvertToString(parameter) }
                    };

                    queryRequest.QueryParameters.Add(queryParameter);
                }
            }

            var request = bigQueryService.Jobs.Query(queryRequest, bigQueryCommand.Connection.ProjectId);
            return request;
        }

        private void ProcessQueryResponse(QueryResponse queryResponse)
        {
            if (queryResponse.JobComplete.HasValue && !queryResponse.JobComplete.Value)
                throw new BigQueryException("Timeout is reached");

            rows = queryResponse.Rows ?? new TableRow[] { };
            schema = queryResponse.Schema;
            fieldsCount = queryResponse.TotalRows != null ? schema?.Fields?.Count ?? default : default;
            enumerator = rows.GetEnumerator();
        }

        private T ChangeValueType<T>(object value, int ordinal)
        {
            return (T)ChangeValueType(value, ordinal);
        }

        private object ChangeValueType(object value, int ordinal)
        {
            if (value == null)
                return null;

            var bigQueryType = BigQueryTypeConverter.ToBigQueryDbType(schema.Fields[ordinal].Type);

            return bigQueryType == BigQueryDbType.Timestamp
                ? UnixTimeStampToDateTime(value)
                : Convert.ChangeType(value, BigQueryTypeConverter.ToType(schema.Fields[ordinal].Type), CultureInfo.InvariantCulture);
        }

        private string PrepareCommandText(BigQueryCommand command)
        {
            return command.CommandType == CommandType.TableDirect
                ? $"SELECT * FROM {GetLead()}{command.Connection.DataSetId}.{command.CommandText}{GetEnd()}"
                : command.CommandText;
        }

        private string GetLead()
        {
            return IsStandardSql ? "`" : "[";
        }

        private string GetEnd()
        {
            return IsStandardSql ? "`" : "]";
        }

        private void DisposeCheck()
        {
            if (disposed)
                throw new ObjectDisposedException("This DataReader has already been disposed.");
        }

        private void RangeCheck(int index)
        {
            if (index < 0 || fieldsCount <= index)
                throw new IndexOutOfRangeException($"{nameof(index)} out of range");
        }

        ~BigQueryDataReader()
        {
            Dispose(false);
        }

        #region static
        private static string ConvertToStringForLegacySql(BigQueryParameter parameter)
        {
            if (parameter?.Value == null)
                return null;

            if (parameter.BigQueryDbType == BigQueryDbType.String)
            {
                var invariantString = parameter.Value.ToInvariantString();
                var trimmedString = invariantString.Substring(0, parameter.Size);
                var escapedString = EscapeString(trimmedString);
                return $"'{escapedString}'";
            }

            var format = "{0}";

            switch (parameter.BigQueryDbType)
            {
                case BigQueryDbType.DateTime:
                    format = "cast('{0}' as DATETIME)";
                    break;

                case BigQueryDbType.Date:
                    format = "cast('{0}' as DATE)";
                    break;

                case BigQueryDbType.Time:
                    format = "cast('{0}' as TIME)";
                    break;

                case BigQueryDbType.Timestamp:
                    format = "cast('{0}' as TIMESTAMP)";
                    break;
            }

            return ConvertToString(parameter).ToInvariantString(format);
        }

        private static string ConvertToString(BigQueryParameter parameter)
        {
            if (parameter?.Value == null)
                return null;

            var format = "{0}";

            switch (parameter.BigQueryDbType)
            {
                case BigQueryDbType.DateTime:
                    format = "{0:yyyy-MM-dd HH:mm:ss.ffffff}";
                    break;

                case BigQueryDbType.Date:
                    format = "{0:yyyy-MM-dd}";
                    break;

                case BigQueryDbType.Time:
                    format = "{0:HH:mm:ss.ffffff}";
                    break;

                case BigQueryDbType.Timestamp:
                    format = "{0:u}";
                    break;
            }

            return parameter.Value.ToInvariantString(format);
        }

        private static string EscapeString(string invariantString)
        {
            return invariantString
                .Replace(@"\", @"\\")
                .Replace("'", @"\'")
                .Replace("\"", @"""");
        }

        private static DateTime UnixTimeStampToDateTime(object timestamp)
        {
            return UnixTimeStampToDateTime(double.Parse(timestamp.ToString(), CultureInfo.InvariantCulture));
        }

        private static DateTime UnixTimeStampToDateTime(double unixTimeStamp)
        {
            var dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(unixTimeStamp);
            return dtDateTime;
        }
        #endregion
    }
}

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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DevExpress.DataAccess.BigQuery
{
    /// <summary>
    ///     A SQL statement to be executed against a BigQuery data source.
    /// </summary>
    public class BigQueryCommand : DbCommand, ICloneable
    {
        private const int defaultTimeout = 30;

        private readonly BigQueryParameterCollection bigQueryParameterCollection = new BigQueryParameterCollection();
        private int commandTimeout;
        private CommandType commandType;

        /// <summary>
        ///     Initializes a new instance of the BigQueryCommand class with the specified prototype.
        /// </summary>
        /// <param name="command">A BigQueryCommand object to clone. </param>
        public BigQueryCommand(BigQueryCommand command)
            : this(command.CommandText, command.Connection)
        {
            commandType = command.CommandType;
            commandTimeout = command.commandTimeout;

            foreach (BigQueryParameter bigQueryParameter in command.Parameters)
                Parameters.Add(bigQueryParameter.Clone());
        }

        /// <summary>
        ///     Initializes a new instance of the BigQueryCommand class with the specified command text and connection settings.
        /// </summary>
        /// <param name="commandText">A System.String values specifying the text of a SQL statement.</param>
        /// <param name="connection">A BigQueryConnection object.</param>
        public BigQueryCommand(string commandText, BigQueryConnection connection)
            : this(commandText)
        {
            Connection = connection;
        }

        /// <summary>
        ///     Initializes a new instance of the BigQueryCommand class with the specified command text.
        /// </summary>
        /// <param name="commandText">A System.String values specifying the text of a SQL statement.</param>
        public BigQueryCommand(string commandText)
        {
            CommandText = commandText;
        }

        /// <summary>
        ///     Initializes a new instance of the BigQueryCommand class with default settings.
        /// </summary>
        public BigQueryCommand() : this(string.Empty)
        {
        }

        /// <summary>
        ///     Specifies the text of a SQL statement to be executed.
        /// </summary>
        /// <value>
        ///     A System.String value specifying the command text.
        /// </value>
        [DefaultValue("")]
        public override string CommandText { get; set; }

        /// <summary>
        ///     Specifies the time before terminating the attempt to execute the command.
        /// </summary>
        /// <value>
        ///     The amount of time in seconds. The default value is 30.
        /// </value>
        [DefaultValue(defaultTimeout)]
        public override int CommandTimeout
        {
            get => commandTimeout;
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value), value, "CommandTimeout can't be less than zero");

                commandTimeout = value;
            }
        }

        /// <summary>
        ///     Specifies how the command should be interpreted.
        /// </summary>
        /// <value>
        ///     A CommandType enumeration value.
        /// </value>
        [DefaultValue(CommandType.Text)]
        public override CommandType CommandType
        {
            get => commandType;
            set
            {
                if (value == CommandType.StoredProcedure)
                    throw new ArgumentOutOfRangeException(nameof(value), value, "BigQuery does not support stored procedures");

                commandType = value;
            }
        }

        /// <summary>
        ///     Specifies how a query command results are applied to a data row being updated. This implementation always throws
        ///     NotSupportedException.
        /// </summary>
        /// <value>
        ///     An UpdateRowSource enumeration value.
        /// </value>
        public override UpdateRowSource UpdatedRowSource
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <summary>
        ///     Specifies the current connection to a BigQuery data source.
        /// </summary>
        /// <value>
        ///     A BigQueryConnection object.
        /// </value>
        [DefaultValue(null)]
        public new BigQueryConnection Connection { get; set; }

        /// <summary>
        ///     Indicates whether the command object should be visible in a customized interface control.
        /// </summary>
        /// <value>
        ///     true, if the command object should be visible in a control; otherwise false.
        /// </value>
        public override bool DesignTimeVisible
        {
            get => false;
            set => throw new NotSupportedException();
        }

        protected override DbConnection DbConnection
        {
            get => Connection;
            set => Connection = (BigQueryConnection)value;
        }

        protected override DbParameterCollection DbParameterCollection => bigQueryParameterCollection;

        protected override DbTransaction DbTransaction { get; set; }

        object ICloneable.Clone()
        {
            return new BigQueryCommand(this);
        }

        /// <summary>
        ///     Creates a prepared (or compiled) version of the command on the data source. This implementation always throws
        ///     NotSupportedException.
        /// </summary>
        public override void Prepare()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///     Asynchronously executes a non-query SQL statement. The asynchronous result contains the number of rows affected.
        /// </summary>
        /// <param name="cancellationToken"> A cancellation token that can be used to cancel command execution.</param>
        /// <returns>
        ///     An asynchronous operation result, containing an Int32 value.
        /// </returns>
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            cancellationToken.Register(Cancel);

            using (var dbDataReader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false))
            {
                while (await dbDataReader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    ;

                return dbDataReader.RecordsAffected;
            }
        }

        /// <summary>
        ///     Executes a non-query SQL statement and returns the number of rows affected.
        /// </summary>
        /// <returns>
        ///     The number of rows affected.
        /// </returns>
        public override int ExecuteNonQuery()
        {
            Task<int> task = ExecuteNonQueryAsync();

            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                throw e.Flatten().InnerException;
            }
        }

        /// <summary>
        ///     Asynchronously executes a SQL statement. The asynchronous result contains the first column of the first row of the
        ///     resulting data.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel command execution.</param>
        /// <returns>An asynchronous operation result, containing an Object value.</returns>
        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            cancellationToken.Register(Cancel);
            object result = null;

            using (var dbDataReader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ConfigureAwait(false))
            {
                if (await dbDataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    if (dbDataReader.FieldCount > 0)
                        result = dbDataReader.GetValue(0);
            }

            return result;
        }

        /// <summary>
        ///     Executes a SQL statement and returns the first column of the first row of the resulting data.
        /// </summary>
        /// <returns>
        ///     The first column of the first row of the resulting data.
        /// </returns>
        public override object ExecuteScalar()
        {
            Task<object> task = ExecuteScalarAsync(CancellationToken.None);

            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                throw e.Flatten().InnerException;
            }
        }

        /// <summary>
        ///     Cancels the execution of the current BigQueryCommand.
        /// </summary>
        public override void Cancel()
        {
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            cancellationToken.Register(Cancel);
            var reader = new BigQueryDataReader(behavior, this, Connection.Service);
            await reader.InitializeAsync(cancellationToken).ConfigureAwait(false);
            return reader;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            Task<DbDataReader> task = ExecuteDbDataReaderAsync(behavior, CancellationToken.None);

            try
            {
                return task.Result;
            }
            catch (AggregateException e)
            {
                throw e.Flatten().InnerException;
            }
        }

        protected override DbParameter CreateDbParameter()
        {
            return new BigQueryParameter();
        }
    }
}

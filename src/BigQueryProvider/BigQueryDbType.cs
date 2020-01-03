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

namespace DevExpress.DataAccess.BigQuery
{
    /// <summary>
    ///     Lists BigQuery data types.
    /// </summary>
    public enum BigQueryDbType
    {
        /// <summary>
        ///     A Unicode character string.
        /// </summary>
        String,

        /// <summary>
        /// </summary>
        Bytes,

        /// <summary>
        ///     A 64-bit integer.
        /// </summary>
        Integer,

        /// <summary>
        ///     A double-precision floating point format.
        /// </summary>
        Float,

        /// <summary>
        ///     A binary type contining a value of either true or false.
        /// </summary>
        Boolean,

        /// <summary>
        ///     A collection of fields.
        /// </summary>
        Record,

        /// <summary>
        ///     A type used to store either a UNIX timestamp or  a date/time string. A value is stored internally as a UNIX
        ///     timestamp.
        /// </summary>
        Timestamp,

        /// <summary>
        /// </summary>
        Date,

        /// <summary>
        /// </summary>
        Time,

        /// <summary>
        /// </summary>
        DateTime,

        /// <summary>
        ///     A type unsupported by BigQuery.
        /// </summary>
        Unknown
    }
}

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
using System.Linq;
using System.Threading;

namespace DevExpress.DataAccess.BigQuery
{
    /// <summary>
    ///     A collection of BigQueryParameter objects.
    /// </summary>
    public class BigQueryParameterCollection : DbParameterCollection
    {
        private readonly List<BigQueryParameter> innerList = new List<BigQueryParameter>();

        private object syncRoot;

        /// <summary>
        ///     Gets the number of parameters in the collection.
        /// </summary>
        /// <value>
        ///     the number of elements in the collection.
        /// </value>
        public override int Count => innerList.Count;

        /// <summary>
        ///     indicates whether access to the current BigQueryParameterCollection collection is synchronized.
        /// </summary>
        /// <value>
        ///     true if access is synchronized; otherwise, false.
        /// </value>
        public override bool IsSynchronized => false;

        /// <summary>
        ///     Gets an object that can be used to synchronize access to the BigQueryParameterCollection.
        /// </summary>
        /// <value>
        ///     An object used to synchronize access to the BigQueryParameterCollection.
        /// </value>
        public override object SyncRoot
        {
            get
            {
                if (syncRoot == null)
                    Interlocked.CompareExchange(ref syncRoot, new object(), null);

                return syncRoot;
            }
        }

        /// <summary>
        ///     Indicates whether the collection is a fixed size.
        /// </summary>
        /// <value>
        ///     true if the collection is a fixed size; otherwise false.
        /// </value>
        public override bool IsFixedSize => false;

        /// <summary>
        ///     Indicates whether the collection is read-only.
        /// </summary>
        /// <value>
        ///     true if the collection is read-only; otherwise false.
        /// </value>
        public override bool IsReadOnly => false;

        private static void ValidateType(object value)
        {
            if (!(value is BigQueryParameter))
                throw new ArgumentException("Invalid parameter type");
        }

        /// <summary>
        ///     Adds a new parameter to the collection.
        /// </summary>
        /// <param name="parameterName">The name of a BigQueryParameter.</param>
        /// <param name="dbType">A DBType enumeration value specifying the data type of the parameter.</param>
        /// <returns>The index of a new BigQuery parameter in the collection.</returns>
        public int Add(string parameterName, DbType dbType)
        {
            return Add(new BigQueryParameter(parameterName, dbType));
        }

        /// <summary>
        ///     returns the index of the specified parameter.
        /// </summary>
        /// <param name="parameterName">The name of a BigQueryParameter.</param>
        /// <returns>the index of the specified BigQueryParameter.</returns>
        public override int IndexOf(string parameterName)
        {
            var value = innerList.FirstOrDefault(p => p.ParameterName == parameterName);
            return IndexOf(value);
        }

        /// <summary>
        ///     returns the index of the specified object.
        /// </summary>
        /// <param name="value">an object to find.</param>
        /// <returns>the index of the specified object.</returns>
        public override int IndexOf(object value)
        {
            if (value == null)
                return -1;

            ValidateType(value);

            for (var i = 0; i < innerList.Count; i++)
                if (innerList[i] == value)
                    return i;

            return -1;
        }

        /// <summary>
        ///     Returns an enumerator used to iterate through the collection.
        /// </summary>
        /// <returns>an object implementing the IEnumerator interface.</returns>
        public override IEnumerator GetEnumerator()
        {
            return innerList.GetEnumerator();
        }

        /// <summary>
        ///     Removes the specified BigQueryParameter form the collection.
        /// </summary>
        /// <param name="value">A BigQueryParameter object.</param>
        public override void Remove(object value)
        {
            ValidateType(value);
            var index = IndexOf(value);

            if (index >= 0)
                RemoveAt(index);
            else
                throw new InvalidOperationException("Item to remove not found");
        }

        /// <summary>
        ///     Removes a BigQueryParameter specified by index from the collection.
        /// </summary>
        /// <param name="index">An index from which to remove an element.</param>
        public override void RemoveAt(int index)
        {
            RangeCheck(index);
            RemoveIndex(index);
        }

        /// <summary>
        ///     Removes a BigQueryParameter specified by name from the collection.
        /// </summary>
        /// <param name="parameterName">The name of a BigQueryParameter.</param>
        public override void RemoveAt(string parameterName)
        {
            RemoveIndex(CheckName(parameterName));
        }

        /// <summary>
        ///     Adds a parameter to the collection.
        /// </summary>
        /// <param name="parameter">a BigQueryParameter object.</param>
        /// <returns>
        ///     The position into which the new element was inserted, or -1 to indicate that the item was not inserted into
        ///     the collection.
        /// </returns>
        public override int Add(object parameter)
        {
            if (parameter == null)
                throw new ArgumentNullException(nameof(parameter));

            ValidateType(parameter);
            innerList.Add((BigQueryParameter)parameter);
            return Count - 1;
        }

        /// <summary>
        ///     Indicates whether or not the current collection contains the specified BigQueryParameter.
        /// </summary>
        /// <param name="value">A BigQueryParameter object.</param>
        /// <returns>true, if the collection contains the specified parameter; otherwise, false.</returns>
        public override bool Contains(object value)
        {
            return IndexOf(value) >= 0;
        }

        /// <summary>
        ///     Removes all items from the collection.
        /// </summary>
        public override void Clear()
        {
            innerList.Clear();
        }

        /// <summary>
        ///     Inserts a BigQueryParameter to the current collection.
        /// </summary>
        /// <param name="index">An index at which to insert an element.</param>
        /// <param name="value">A BigQueryParameter to insert.</param>
        public override void Insert(int index, object value)
        {
            ValidateType(value);
            innerList.Insert(index, (BigQueryParameter)value);
        }

        /// <summary>
        ///     Indicates whether or not the current collection contains the specified BigQueryParameter.
        /// </summary>
        /// <param name="parameterName">The name of a BigQueryParameter.</param>
        /// <returns>true, if the collection contains the specified parameter; otherwise, false.</returns>
        public override bool Contains(string parameterName)
        {
            return IndexOf(parameterName) != -1;
        }

        /// <summary>
        ///     Copies the element of the current collection to the specified position of an Array.
        /// </summary>
        /// <param name="array">An zero-based Array to which to copy  the elements of the collection.</param>
        /// <param name="index">An index within an Array at which to start copying.</param>
        public override void CopyTo(Array array, int index)
        {
            ((ICollection)innerList).CopyTo(array, index);
        }

        /// <summary>
        ///     Adds an array of values to the current collection.
        /// </summary>
        /// <param name="values">an array of BigQueryParameter objects.</param>
        public override void AddRange(Array values)
        {
            innerList.AddRange(values.OfType<BigQueryParameter>().ToArray());
        }

        internal void Validate()
        {
            CheckDuplicateNames();

            foreach (var parameter in innerList)
                parameter.Validate();
        }

        protected override DbParameter GetParameter(int index)
        {
            RangeCheck(index);
            return innerList[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            var index = IndexOf(parameterName);

            if (index < 0)
                throw new IndexOutOfRangeException();

            return innerList[index];
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            ValidateType(value);
            RangeCheck(index);
            Replace(index, value);
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);

            if (index < 0)
                throw new ArgumentException("Wrong parameter name");

            Replace(index, value);
        }

        private void RemoveIndex(int index)
        {
            innerList.RemoveAt(index);
        }

        private void RangeCheck(int index)
        {
            if (index < 0 || Count <= index)
                throw new IndexOutOfRangeException();
        }

        private int CheckName(string parameterName)
        {
            var index = IndexOf(parameterName);

            if (index < 0)
                throw new ArgumentException("Wrong parameter name");

            return index;
        }

        private void ValidateParameter(int index, DbParameter value)
        {
            if (value == null)
                throw new NullReferenceException("parameter");

            if (index == IndexOf(value))
                return;

            if (!string.IsNullOrEmpty(value.ParameterName))
                return;

            string parameterName;
            index = 1;

            do
            {
                parameterName = "Parameters" + index.ToString(CultureInfo.CurrentCulture);
                index++;
            } while (IndexOf(parameterName) != -1);

            value.ParameterName = parameterName;
        }

        private void Replace(int index, DbParameter value)
        {
            ValidateType(value);
            ValidateParameter(index, value);
            innerList[index] = (BigQueryParameter)value;
        }

        private void CheckDuplicateNames()
        {
            var set = new HashSet<string>();

            foreach (var bigQueryParameter in innerList)
            {
                if (set.Contains(bigQueryParameter.ParameterName))
                    throw new DuplicateNameException("Parameter collection contains duplicate parameters with name '" + bigQueryParameter.ParameterName + "'");

                set.Add(bigQueryParameter.ParameterName);
            }
        }
    }
}

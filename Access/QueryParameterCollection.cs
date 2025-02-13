using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace Public.DB
{
    /// <summary>
    /// 表示查询参数的集合，支持忽略大小写的参数名称匹配。
    /// </summary>
    [Serializable]
    public class QueryParameterCollection : MarshalByRefObject, IEnumerable<QueryParameter>
    {
        private readonly SortedList<string, QueryParameter> _params = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// 获取集合中包含的参数数量。
        /// </summary>
        public int Count => _params.Count;

        /// <summary>
        /// 根据索引获取或设置集合中的参数。
        /// </summary>
        /// <param name="index">要获取或设置的参数索引。</param>
        /// <exception cref="IndexOutOfRangeException">如果索引超出范围。</exception>
        public QueryParameter this[int index]
        {
            get
            {
                if (index < 0 || Count <= index)
                {
                    throw new IndexOutOfRangeException($"Index {index} is out of range.");
                }
                var key = _params.Keys[index];
                return _params[key];
            }
            set
            {
                if (index < 0 || Count <= index)
                {
                    throw new IndexOutOfRangeException($"Index {index} is out of range.");
                }
                var key = _params.Keys[index];
                _params[key] = value;
            }
        }

        /// <summary>
        /// 根据参数名称获取或设置集合中的参数。
        /// </summary>
        /// <param name="parameterName">要获取或设置的参数名称。</param>
        /// <exception cref="KeyNotFoundException">如果未找到指定名称的参数。</exception>
        public QueryParameter this[string parameterName]
        {
            get
            {
                if (!_params.ContainsKey(parameterName))
                    throw new KeyNotFoundException($"Parameter '{parameterName}' does not exist.");
                return _params[parameterName];
            }
            set => _params[parameterName] = value;
        }

        /// <summary>
        /// 向集合中添加一个新的查询参数。
        /// </summary>
        /// <param name="param">要添加的查询参数。</param>
        /// <returns>已添加的查询参数实例。</returns>
        public QueryParameter Add(QueryParameter param)
        {
            _params[param.ParameterName] = param;
            return param;
        }

        /// <summary>
        /// 使用指定的名称和值向集合中添加一个新的查询参数。
        /// </summary>
        /// <param name="parameterName">参数名称。</param>
        /// <param name="value">参数值。</param>
        /// <returns>已添加的查询参数实例。</returns>
        public QueryParameter Add(string parameterName, object value)
        {
            return Add(new QueryParameter(parameterName, value));
        }

        /// <summary>
        /// 使用指定的名称、值和数据库类型向集合中添加一个新的查询参数。
        /// </summary>
        /// <param name="parameterName">参数名称。</param>
        /// <param name="value">参数值。</param>
        /// <param name="dbType">数据库类型。</param>
        /// <returns>已添加的查询参数实例。</returns>
        public QueryParameter Add(string parameterName, object value, DbType dbType)
        {
            return Add(new QueryParameter(parameterName, value, dbType));
        }

        /// <summary>
        /// 清空集合中的所有参数。
        /// </summary>
        public void Clear()
        {
            _params.Clear();
        }

        /// <summary>
        /// 返回一个枚举器，用于遍历集合中的每个元素。
        /// </summary>
        /// <returns>一个可用于遍历集合的枚举器。</returns>
        public IEnumerator<QueryParameter> GetEnumerator()
        {
            return _params.Values.GetEnumerator();
        }

        /// <summary>
        /// 返回一个非泛型枚举器，用于遍历集合中的每个元素。
        /// </summary>
        /// <returns>一个可用于遍历集合的非泛型枚举器。</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
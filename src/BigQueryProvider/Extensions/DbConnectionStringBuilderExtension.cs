using System.Data.Common;

namespace DevExpress.DataAccess.BigQuery.Extensions
{
    public static class DbConnectionStringBuilderExtension
    {
        public static object SafeGetValue(this DbConnectionStringBuilder builder, string value, object defaultValue = null)
        {
            builder.TryGetValue(value, out var result);
            return result ?? defaultValue;
        }

        public static bool SafeSetValue(this DbConnectionStringBuilder builder, string key, object value)
        {
            if (SafeGetValue(builder, key) != value)
            {
                builder[key] = value;
                return true;
            }

            return false;
        }
    }
}

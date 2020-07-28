using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests
{
    public class EnumCacheTests
    {
        [StoreAsText]
        public enum TestEnumStoreAsText
        {
            Value1,

            Value2,

            Value3
        }

        public enum TestEnumStoreAsInt
        {
            Value1,

            Value2,

            Value3
        }

        public enum TestByteEnumStoreAsInt : byte
        {
            Value1,

            Value2,

            Value3
        }

        public enum TestEnumWithRepeats
        {
            Value1 = 1,

            Value2 = 2,

            Value2Again = 2,

            Value3 = 3,
        }

        [StoreAsText]
        public enum TestEnumWithRepeatsAsText
        {
            Value1 = 1,

            Value2 = 2,

            Value2Again = 2,

            Value3 = 3,
        }

        public class TestClassThusNotEnum
        {
        }

        [Fact]
        public void ShouldReturnTrueForEnumStoreAsText()
        {
            var info = EnumCache.GetInfo<TestEnumStoreAsText>();

            Assert.True(info.IsEnum);
            Assert.True(info.StoreAsText);
            Assert.NotNull(info.EnumValues);

            var values = Enum.GetValues(typeof(TestEnumStoreAsText)).Cast<object>().ToList();

            for (int i = 0; i < values.Count; i++)
            {
                Assert.Equal(values[i].ToString(), info.EnumValues[i]);
            }
        }

        [Fact]
        public void ShouldReturnTrueForEnumStoreAsInt()
        {
            var info = EnumCache.GetInfo<TestEnumStoreAsInt>();

            Assert.True(info.IsEnum);
            Assert.False(info.StoreAsText);
            Assert.Null(info.EnumValues);
        }

        [Fact]
        public void ShouldReturnTrueForByteEnumStoreAsInt()
        {
            var info = EnumCache.GetInfo<TestByteEnumStoreAsInt>();

            Assert.True(info.IsEnum);
            Assert.False(info.StoreAsText);
        }

        [Fact]
        public void ShouldReturnFalseForClass()
        {
            var info = EnumCache.GetInfo<TestClassThusNotEnum>();

            Assert.False(info.IsEnum);
            Assert.False(info.StoreAsText);
            Assert.Null(info.EnumValues);
        }

        [Fact]
        public void EnumsWithRepeatedValues()
        {
            var info = EnumCache.GetInfo<TestEnumWithRepeats>();

            Assert.True(info.IsEnum);
            Assert.False(info.StoreAsText);
            Assert.Null(info.EnumValues);
        }

        [Fact]
        public void EnumsWithRepeatedValuesAsText()
        {
            var info = EnumCache.GetInfo<TestEnumWithRepeatsAsText>();

            Assert.True(info.IsEnum);
            Assert.True(info.StoreAsText);
            Assert.NotNull(info.EnumValues);
        }
    }
}

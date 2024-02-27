#include <iostream>

#include <Columns/ColumnVector.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/MergeTreeRangeReader.cpp> // NOLINT

using namespace DB;

void measureAndFilters(size_t size)
{
    auto generateFastIncrementColumn = [](size_t len)->ColumnPtr
    {
        auto filter = ColumnUInt8::create(len);
        auto & filter_data = filter->getData();

        for (size_t i = 0; i < len; ++i)
            filter_data[i] = static_cast<UInt8>(i & 0xFF);

        return filter;
    };

    auto generateSlowIncrementColumn = [](size_t len)->ColumnPtr
    {
        auto filter = ColumnUInt8::create(len);
        auto & filter_data = filter->getData();

        for (size_t i = 0; i < len; ++i)
            filter_data[i] = static_cast<UInt8>((i >> 8) & 0xFF);

        return filter;
    };

    auto first_filter = generateFastIncrementColumn(size);
    auto second_filter = generateSlowIncrementColumn(size);

    {
        Stopwatch watch;
        auto result = andFilters(first_filter, second_filter);
        std::cerr << watch.elapsedSeconds() << std::endl;
    }
}

int main()
{
    size_t size = 100000000;
    measureAndFilters(size);
}

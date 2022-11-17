#include <iostream>
#include <limits>
#include <vector>

#include <Columns/ColumnsNumber.h>
#include <Common/Stopwatch.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsLogical.cpp>

struct LinearCongruentialGenerator
{
    /// Constants from `man lrand48_r`.
    static constexpr UInt64 a = 0x5DEECE66D;
    static constexpr UInt64 c = 0xB;

    /// And this is from `head -c8 /dev/urandom | xxd -p`
    UInt64 current = 0x09826f4a081cee35ULL;

    UInt32 next()
    {
        current = current * a + c;
        return static_cast<UInt32>(current >> 16);
    }
};

void generateRandomUInt8Column(LinearCongruentialGenerator & gen, UInt8 * output, size_t size, double zero_ratio)
{
    /// The LinearCongruentialGenerator generates nonnegative integers uniformly distributed over the interval [0, 2^32).
    /// See https://linux.die.net/man/3/nrand48
    UInt32 threshold = static_cast<UInt32>(static_cast<double>(std::numeric_limits<UInt32>::max()) * zero_ratio);

    for (UInt8 * end = output + size; output != end; ++output)
    {
        UInt32 val = gen.next();
        *output = val > threshold ? 1 : 0;
    }
}

using namespace DB;

template <typename Op, typename OpName, size_t N = 8>
void measureAssociativeApplierPerf(size_t size, double zero_ratio)
{
    LinearCongruentialGenerator gen;

    for (size_t width = 1; width <= N; ++width)
    {
        UInt8ColumnPtrs uint8_args;
        auto col_res = ColumnUInt8::create(size);

        for (size_t i = 0; i < width; ++i)
        {
            auto col = ColumnUInt8::create();
            auto & col_data = col->getData();
            col_data.resize(size);

            generateRandomUInt8Column(gen, col_data.data(), size, zero_ratio);

            uint8_args.push_back(col.get());
        }

        {
            Stopwatch watch;
            OperationApplier<Op, AssociativeApplierImpl>::apply(uint8_args, col_res->getData(), false);
            std::cerr << OpName::name << " operation on " << width << " columns with the zero ratio of " << zero_ratio << " elapsed: " << watch.elapsedSeconds() << std::endl;
        }
    }
}

template <typename Op, typename OpName, size_t N = 8>
void measureAssociativeGenericApplierPerf(size_t size, double zero_ratio, double null_ratio)
{
    LinearCongruentialGenerator gen;
    double non_null_ratio = 1 - null_ratio;

    for (size_t width = 1; width <= N; ++width)
    {
        ColumnRawPtrs arguments;
        auto col_res = ColumnUInt8::create(size);

        for (size_t i = 0; i < width; ++i)
        {
            auto nested_col = ColumnUInt8::create(size);
            auto null_map = ColumnUInt8::create(size);
            auto & nested_col_data = nested_col->getData();
            auto & null_map_data = null_map->getData();

            generateRandomUInt8Column(gen, null_map_data.data(), size, non_null_ratio);
            generateRandomUInt8Column(gen, nested_col_data.data(), size, zero_ratio / non_null_ratio);
            
            auto col_nullable = ColumnNullable::create(std::move(nested_col), std::move(null_map));

            arguments.push_back(col_nullable.get());
        }

        {
            Stopwatch watch;
            OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, col_res->getData(), false);
            std::cerr << OpName::name << " operation on " << width << " columns with the zero ratio of " << zero_ratio << " and null ratio of " << null_ratio << " elapsed: " << watch.elapsedSeconds() << std::endl;
        }
    }
}

int main()
{
    size_t size = 10000000;

    //std::cerr << "Meaure Performance of AssociativeApplier" << std::endl;
    //for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    //{
    //    measureAssociativeApplierPerf<AndImpl, NameAnd>(size, zero_ratio);
    //    measureAssociativeApplierPerf<OrImpl, NameOr>(size, zero_ratio);
    //}

    std::cerr << "Meaure Performance of AssociativeGenericApplier" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = zero_ratio;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr>(size, zero_ratio, null_ratio);
    }
}

#include <iostream>
#include <limits>
#include <vector>
#include <string>

#include <Columns/ColumnNothing.h>
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

template<typename T>
void generateRandomColumn(LinearCongruentialGenerator & gen, T * output, size_t size, double zero_ratio)
{
    /// The LinearCongruentialGenerator generates nonnegative integers uniformly distributed over the interval [0, 2^32).
    /// See https://linux.die.net/man/3/nrand48
    UInt32 threshold = static_cast<UInt32>(static_cast<double>(std::numeric_limits<UInt32>::max()) * zero_ratio);

    for (T * end = output + size; output != end; ++output)
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

            generateRandomColumn(gen, col_data.data(), size, zero_ratio);

            uint8_args.push_back(col.get());
        }

        {
            Stopwatch watch;
            OperationApplier<Op, AssociativeApplierImpl>::apply(uint8_args, col_res->getData(), false);
            std::cerr << OpName::name << " operation on " << width << " columns with the zero ratio of " << zero_ratio << " elapsed: " << watch.elapsedSeconds() << std::endl;
        }
    }
}

template <typename Op, typename OpName, typename T, size_t N = 8>
void measureAssociativeGenericApplierPerf(size_t size, double zero_ratio, double null_ratio)
{
    LinearCongruentialGenerator gen;
    double non_null_ratio = 1 - null_ratio;

    for (size_t width = 1; width <= N; ++width)
    {
        ColumnRawPtrs arguments;
        auto col_res = ColumnUInt8::create(size);

        if (null_ratio == 0)
        {
            for (size_t i = 0; i < width; ++i)
            {
                auto col = ColumnUInt8::create();
                auto & col_data = col->getData();
                col_data.resize(size);

                generateRandomColumn(gen, col_data.data(), size, zero_ratio);

                arguments.push_back(col.get());
            }
        }
        else if (null_ratio == 0)
        {
            for (size_t i = 0; i < width; ++i)
            {
                auto nested_col = ColumnNothing::create(size);
                auto null_map = ColumnUInt8::create(size);
                auto col_nullable = ColumnNullable::create(std::move(nested_col), std::move(null_map));

                arguments.push_back(col_nullable.get());
            }
        }
        else
        {
            for (size_t i = 0; i < width; ++i)
            {
                auto nested_col = ColumnVector<T>::create(size);
                auto null_map = ColumnUInt8::create(size);
                auto & nested_col_data = nested_col->getData();
                auto & null_map_data = null_map->getData();

                generateRandomColumn(gen, null_map_data.data(), size, non_null_ratio);
                generateRandomColumn(gen, nested_col_data.data(), size, zero_ratio / non_null_ratio);
                
                auto col_nullable = ColumnNullable::create(std::move(nested_col), std::move(null_map));

                arguments.push_back(col_nullable.get());
            }
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

    std::cerr << "Meaure Performance of AssociativeApplier" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        measureAssociativeApplierPerf<AndImpl, NameAnd>(size, zero_ratio);
        measureAssociativeApplierPerf<OrImpl, NameOr>(size, zero_ratio);
    }

    std::cerr << "Measure Performance of AssociativeGenericApplier" << std::endl;
    std::cerr << "UInt8" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt8>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt8>(size, zero_ratio, null_ratio);
    }
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 1;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt8>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt8>(size, zero_ratio, null_ratio);
    }
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt8>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt8>(size, zero_ratio, null_ratio);
    }

    std::cerr << "UInt16" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt16>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt16>(size, zero_ratio, null_ratio);
    }

    std::cerr << "UInt32" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt32>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt32>(size, zero_ratio, null_ratio);
    }

    std::cerr << "UInt64" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt64>(size / 2, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt64>(size / 2, zero_ratio, null_ratio);
    }

    std::cerr << "Int8" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int8>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int8>(size, zero_ratio, null_ratio);
    }

    std::cerr << "Int16" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int16>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int16>(size, zero_ratio, null_ratio);
    }

    std::cerr << "Int32" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int32>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int32>(size, zero_ratio, null_ratio);
    }

    std::cerr << "Int64" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int64>(size / 2, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int64>(size / 2, zero_ratio, null_ratio);
    }

    std::cerr << "Float32" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Float32>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Float32>(size, zero_ratio, null_ratio);
    }

    std::cerr << "Float64" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.05;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Float64>(size / 2, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Float64>(size / 2, zero_ratio, null_ratio);
    }
}

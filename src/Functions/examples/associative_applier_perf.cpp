#include <iostream>
#include <limits>
#include <vector>
#include <string>

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

        {
            Stopwatch watch;
            OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, col_res->getData(), false);
            std::cerr << OpName::name << " operation on " << width << " columns with the zero ratio of " << zero_ratio << " and null ratio of " << null_ratio << " elapsed: " << watch.elapsedSeconds() << std::endl;
        }
    }
}

template <typename Op, typename OpName, typename T>
void testAssociativeGenericApplier()
{
    size_t size = 9;
    
    ColumnRawPtrs arguments;
    auto col_res = ColumnUInt8::create(size);
    auto & col_res_data = col_res->getData();
    auto col_expected = ColumnUInt8::create(size);
    auto & col_expected_data = col_expected->getData();
    
    if (std::string(OpName::name) == std::string("and"))
    {
        col_expected_data[0] = Ternary::False;
        col_expected_data[1] = Ternary::False;
        col_expected_data[2] = Ternary::False;
        col_expected_data[3] = Ternary::False;
        col_expected_data[4] = Ternary::Null;
        col_expected_data[5] = Ternary::Null;
        col_expected_data[6] = Ternary::False;
        col_expected_data[7] = Ternary::Null;
        col_expected_data[8] = Ternary::True;
    }
    else if (std::string(OpName::name) == std::string("or"))
    {
        col_expected_data[0] = Ternary::False;
        col_expected_data[1] = Ternary::Null;
        col_expected_data[2] = Ternary::True;
        col_expected_data[3] = Ternary::Null;
        col_expected_data[4] = Ternary::Null;
        col_expected_data[5] = Ternary::True;
        col_expected_data[6] = Ternary::True;
        col_expected_data[7] = Ternary::True;
        col_expected_data[8] = Ternary::True;
    }
    
    UInt8 ternary_values[] = {Ternary::False, Ternary::Null, Ternary::True}; 
    
    auto nested_col_a = ColumnVector<T>::create(size);
    auto null_map_a = ColumnUInt8::create(size);
    auto & nested_col_data_a = nested_col_a->getData();
    auto & null_map_data_a = null_map_a->getData();
    
    auto nested_col_b = ColumnVector<T>::create(size);
    auto null_map_b = ColumnUInt8::create(size);
    auto & nested_col_data_b = nested_col_b->getData();
    auto & null_map_data_b = null_map_b->getData();

    for (size_t i = 0; i < 3; ++i)
    {
        for (size_t j = 0; j < 3; ++j)
        {
            //Column a
            if (ternary_values[i] == Ternary::Null)
            {
                null_map_data_a[3*i+j] = 1;
                nested_col_data_a[3*i+j] = 0;
            }
            else if (ternary_values[i] == Ternary::True)
            {
                null_map_data_a[3*i+j] = 0;
                nested_col_data_a[3*i+j] = 1;
            }
            else if (ternary_values[i] == Ternary::False)
            {
                null_map_data_a[3*i+j] = 0;
                nested_col_data_a[3*i+j] = 0;
            }

            //Column b
            if (ternary_values[j] == Ternary::Null)
            {
                null_map_data_b[3*i+j] = 1;
                nested_col_data_b[3*i+j] = 0;
            }
            else if (ternary_values[j] == Ternary::True)
            {
                null_map_data_b[3*i+j] = 0;
                nested_col_data_b[3*i+j] = 1;
            }
            else if (ternary_values[j] == Ternary::False)
            {
                null_map_data_b[3*i+j] = 0;
                nested_col_data_b[3*i+j] = 0;
            }
        }
    }
    
    auto col_nullable_a = ColumnNullable::create(std::move(nested_col_a), std::move(null_map_a));
    auto col_nullable_b = ColumnNullable::create(std::move(nested_col_b), std::move(null_map_b));
    
    arguments.push_back(col_nullable_a.get());
    arguments.push_back(col_nullable_b.get());

    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, col_res->getData(), false);
    
    for (size_t i = 0; i < size; ++i)
    {
        if (col_res_data[i] != col_expected_data[i])
        {
            std::cerr << "Result error: operator" << OpName::name << " index:" << i << std::endl;
        }
    }
    
}

int main()
{
    size_t size = 10000000;

    testAssociativeGenericApplier<AndImpl, NameAnd, UInt8>();
    testAssociativeGenericApplier<OrImpl, NameOr, UInt8>();
    //std::cerr << "Meaure Performance of AssociativeApplier" << std::endl;
    //for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    //{
    //    measureAssociativeApplierPerf<AndImpl, NameAnd>(size, zero_ratio);
    //    measureAssociativeApplierPerf<OrImpl, NameOr>(size, zero_ratio);
    //}

    std::cerr << "Meaure Performance of AssociativeGenericApplier" << std::endl;
    std::cerr << "UInt8" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt8>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt8>(size, zero_ratio, null_ratio);
    }

    std::cerr << "UInt16" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt16>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt16>(size, zero_ratio, null_ratio);
    }

    std::cerr << "UInt32" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt32>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt32>(size, zero_ratio, null_ratio);
    }

    //std::cerr << "UInt64" << std::endl;
    //for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    //{
    //    double null_ratio = 0.0;
    //    measureAssociativeGenericApplierPerf<AndImpl, NameAnd, UInt64>(size, zero_ratio, null_ratio);
    //    measureAssociativeGenericApplierPerf<OrImpl, NameOr, UInt64>(size, zero_ratio, null_ratio);
    //}

    std::cerr << "Int8" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int8>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int8>(size, zero_ratio, null_ratio);
    }

    std::cerr << "Int16" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int16>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int16>(size, zero_ratio, null_ratio);
    }

    std::cerr << "Int32" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int32>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int32>(size, zero_ratio, null_ratio);
    }

    //std::cerr << "Int64" << std::endl;
    //for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    //{
    //    double null_ratio = 0.0;
    //    measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Int64>(size, zero_ratio, null_ratio);
    //    measureAssociativeGenericApplierPerf<OrImpl, NameOr, Int64>(size, zero_ratio, null_ratio);
    //}

    std::cerr << "Float32" << std::endl;
    for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    {
        double null_ratio = 0.0;
        measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Float32>(size, zero_ratio, null_ratio);
        measureAssociativeGenericApplierPerf<OrImpl, NameOr, Float32>(size, zero_ratio, null_ratio);
    }

    //std::cerr << "Float64" << std::endl;
    //for (double zero_ratio = 0.0; zero_ratio < 1.1; zero_ratio += 0.2)
    //{
    //    double null_ratio = 0.0;
    //    measureAssociativeGenericApplierPerf<AndImpl, NameAnd, Float64>(size, zero_ratio, null_ratio);
    //    measureAssociativeGenericApplierPerf<OrImpl, NameOr, Float64>(size, zero_ratio, null_ratio);
    //}
}

#include <fstream>
#include <cstdint>
#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <thread>

std::vector<uint32_t> readBlock(std::ifstream& inputStream, size_t blockSize)
{
    if (inputStream.eof())
        return {};

    std::vector<uint32_t> values;
    values.resize(blockSize);

    inputStream.read(reinterpret_cast<char*>(&values[0]), blockSize * sizeof(uint32_t));

    values.resize(inputStream.gcount() / sizeof(uint32_t));

    return values;
}

void writeBlock(std::vector<uint32_t>&& values, std::string&& name)
{
    std::ofstream out(name, std::ios::out | std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(&values[0]), values.size() * sizeof(uint32_t));
}


void merge(const std::string& filename1, const std::string& filename2, size_t blockSize)
{
    const char *tempFilename = "temp";

    { // <-- block for closing fstreams
        std::ifstream file1(filename1, std::ios::binary);
        std::ifstream file2(filename2, std::ios::binary);

        std::ofstream tempFile(tempFilename, std::ios::binary | std::ios::trunc);

        bool firstMerge = true;
        uint32_t val1, val2;

        // if both files are not empty we merge them comparing one-by-one from each file
        if (file1.peek() != std::ifstream::traits_type::eof() && file2.peek() != std::ifstream::traits_type::eof())
        {
            firstMerge = false;

            file1.read(reinterpret_cast<char *>(&val1), sizeof(val1));
            file2.read(reinterpret_cast<char *>(&val2), sizeof(val2));

            while (!file1.eof() && !file2.eof()) {
                if (val1 >= val2) {
                    tempFile.write(reinterpret_cast<const char *>(&val2), sizeof(val2));
                    file2.read(reinterpret_cast<char *>(&val2), sizeof(val2));
                } else {
                    tempFile.write(reinterpret_cast<const char *>(&val1), sizeof(val1));
                    file1.read(reinterpret_cast<char *>(&val1), sizeof(val1));
                }
            }
        }


        // if first file is done or empty we copy rest of second file to temp result file
        if (file1.eof() && !file2.eof())
        {
            if (!firstMerge)
                tempFile.write(reinterpret_cast<const char *>(&val1), sizeof(val1)); // write previously read number

            std::vector<uint32_t> rest;
            rest.resize(blockSize);
            file2.read(reinterpret_cast<char *>(&rest[0]), rest.size() * sizeof(uint32_t));
            rest.resize(file2.gcount() / sizeof(uint32_t));

            tempFile.write(reinterpret_cast<const char *>(&rest[0]), rest.size() * sizeof(uint32_t));
        }
        // if second file is done or empty we copy rest of first file to temp result file
        else if (file2.eof() && !file1.eof())
        {
            if (!firstMerge)
                tempFile.write(reinterpret_cast<const char *>(&val2), sizeof(val2)); // write previously read number

            std::vector<uint32_t> rest;
            rest.resize(blockSize);
            file1.read(reinterpret_cast<char *>(&rest[0]), rest.size() * sizeof(uint32_t));
            rest.resize(file1.gcount() / sizeof(uint32_t));

            tempFile.write(reinterpret_cast<const char *>(&rest[0]), rest.size() * sizeof(uint32_t));
        }
    }

    std::remove(filename1.c_str());
    std::remove(filename2.c_str());
    std::rename(tempFilename, filename1.c_str());
}


int main()
{
    constexpr size_t MEMORY_SIZE = 10*1024*1024;
    //const auto threadCount = std::thread::hardware_concurrency() - 1;
    const auto threadCount = 2- 1;
    const size_t blockSize = MEMORY_SIZE / threadCount / sizeof(uint32_t);

    std::vector<uint8_t> tempFiles(threadCount);

    std::ifstream infile("input", std::ios::binary);
    if (!infile)
    {
        //TODO: handle the error
        return 1;
    }

    const char* outputFilename = "output";
    std::ofstream outfile(outputFilename, std::ios::binary);
    if (!outfile)
    {
        //TODO: handle the error
        return 1;
    }
    outfile.close();

    std::vector<std::thread> threads;

    bool stop = false;
    while(true)
    {
        std::fill(tempFiles.begin(), tempFiles.end(), 0);
        threads.clear();

        for(size_t i = 0; i < threadCount; ++i)
        {
            auto block = readBlock(infile, blockSize);
            if (block.empty())
            {
                stop = true;
                break;
            }

            threads.emplace_back([&tempFiles,i](std::vector<uint32_t>&& block)
                                 {
                                     std::sort(block.begin(), block.end());
                                     writeBlock(std::move(block), std::to_string(i));
                                     tempFiles[i] = 1;
                                 }, std::move(block));

        }

        for(auto& t : threads)
        {
            t.join();
        }

        for(int i = 0; i < tempFiles.size(); ++i)
        {
            if(tempFiles[i] == 1)
            {
                merge(outputFilename, std::to_string(i), blockSize);
            }
        }

        if (stop)
            break;
    }

    std::cout << "Finished\n";
}


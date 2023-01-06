//
// Created by Pretorius, Christiaan on 2020-12-26.
// simple C++ 17 library to print to the console safer and easier than either cout or printf
//

#ifndef REPLIFS_LLINE_H
#define REPLIFS_LLINE_H
#include <iostream>
#include <string>
#include <vector>
#include <variant>

namespace console {
    typedef std::variant<const std::string, uint64_t, int64_t ,size_t, unsigned int, int, long, double, float> _LogValue;
    /**
     * converts the variant type _LogValue to a string
     * if a new type was added to _LogValue without changing the function <unknown> is returned
     * @param val the variant of type _LogValue
     * @return a std::string
     */
    static inline std::string to_string(const _LogValue& val){
       
        if(const std::string* s = std::get_if<const std::string>(&val)){
            return *s;
        }
        if(const long*  l= std::get_if<long>(&val)){
            return std::to_string(*l);
        }
        if(const int*  i= std::get_if<int>(&val)){
            return std::to_string(*i);
        }
        if(const uint64_t*  ull= std::get_if<uint64_t>(&val)){
            return std::to_string(*ull);
        }
        if(const int64_t* ll= std::get_if<int64_t>(&val)){
            return std::to_string(*ll);
        }
        if(const size_t* z= std::get_if<size_t>(&val)){
            return std::to_string(*z);
        }
        if(const unsigned int* z= std::get_if<unsigned int>(&val)){
            return std::to_string(*z);
        }
        if(const double* d= std::get_if<double>(&val)){
            return std::to_string(*d);
        }
        if(const float* f= std::get_if<float>(&val)){
            return std::to_string(*f);
        }
        std::string result = "<unknown>";
        return result;
    }
    /**
     * concatenate values of different types collected as a vector into a string
     * @param values a vector of variants that can contain different types
     * @param seperator string to put between values - enoty if not specified
     * @return the string of concatenated values
     */
    static inline std::string cat(const std::vector<_LogValue>& values, const std::string& separator = ""){
        std::string result;
        size_t at = 0;
        for(auto val : values){
            if(at++ > 0){
                result += separator;
            }
            result += to_string(val);
        }
        
        return result;
    }
    
     /**
      * writes values of different types collected as a vector into std::cout
      * @param values of different variant types
      * @param seperator - defaults to empty string
      */
    static inline void println(const std::vector<_LogValue>& values, const std::string& separator = ""){
        std::string ln = cat(values, separator);
        std::cout << ln << std::endl;
    }
    /**
      * writes values of different types collected as a vector into std::cerr
      * @param values of different variant types
      * @param seperator - defaults to empty string
      */
    static inline void errorln(const std::vector<_LogValue>& values, const std::string& separator = ""){
        std::string ln = cat(values, separator);
        std::cerr << ln << std::endl;
    }
};

#endif //REPLIFS_LLINE_H

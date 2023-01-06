#ifndef _STX_STORAGE_TYPES_H_
#define _STX_STORAGE_TYPES_H_

#include <type_traits>
#include "logging/console.h"
template<typename T, typename NameGetter>
class has_member_impl {
private:
    typedef char matched_return_type;
    typedef long unmatched_return_type;

    template<typename C>
    static matched_return_type f(typename NameGetter::template get<C> *);

    template<typename C>
    static unmatched_return_type f(...);

public:
    static const bool value = (sizeof(f<T>(0)) == sizeof(matched_return_type));
};

template<typename T, typename NameGetter>
struct has_member
        : std::integral_constant<bool, has_member_impl<T, NameGetter>::value> {
};
/**
 * NOTE on store: vector is use as a contiguous array. If a version of STL
 * is encountered that uses a different allocation scheme then a replacement
 * will be provided here.
 */

/*
* Base types and imperfect hash Template Classes v 0.1
*/
/*****************************************************************************

Copyright (c) 2013, Christiaan Pretorius

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/
#include <vector>

#include <stdio.h>
#include <string>
/// files that use these macros need to include <iostream> or <stdio.h>
#define __LOG_SPACES__ "SPACES"
#define __LOG_NAME__ __LOG_SPACES__

namespace persist {
    namespace storage {
        extern bool storage_debugging;
        extern bool storage_info;
        /// unsigned integer primitive types
        typedef std::uint8_t u8;
        typedef std::uint16_t u16;
        typedef std::uint32_t u32;
        typedef std::uint64_t u64;
        /// signed integer primitive types
        typedef std::int8_t i8;
        typedef std::int16_t i16;
        typedef std::int32_t i32;
        typedef std::int64_t i64;
        typedef long long int lld; /// cast for %lld
        typedef double f64;
        /// virtual allocator address type
        typedef u64 stream_address;

        /// the version type
        struct version_type {
            u64 val{0};

            bool is_nonzero() const {
                return val != 0;
            }
        };

        /// format spec casts
        typedef long long unsigned int fi64; /// cast for %llu

        inline const char *tostring(const version_type &v) {
            thread_local std::string r;
            r = std::to_string(v.val);
            return r.c_str();
        };

        inline version_type create_version() {
            static version_type cv{0};

            ++cv.val; //Poco::UUIDGenerator::defaultGenerator().create();
            return cv;
        }

        /// some resource descriptors for low latency verification
        typedef std::pair<stream_address, version_type> resource_descriptor;
        typedef std::vector<resource_descriptor> resource_descriptors;

        inline std::string current_time() {
            static std::string r = "TODO: CTIME";
            return r; //Poco::DateTimeFormatter::format(Poco::Timestamp(),"%Y%m%d %H:%M:%s");
        }

    };
    extern bool memory_low_state;
};
namespace nst = persist::storage;
namespace std {
#if 0 // might be used
    static const char *to_c_string(const nst::version_type &v) {
        thread_local std::string result;
        result = std::to_string(v.val);
        return result.c_str();
    }

    static std::string to_string(const nst::version_type &v) {
        thread_local std::string result;
        result = std::to_string(v.val);
        return result;
    }

    static const char *to_c_string(const typename nst::resource_descriptor &v) {
        thread_local std::string result;
        result = "{";
        result += std::to_string(v.first);
        result += std::to_string(v.second);
        result += "}";
        return result.c_str();
    }

    static const char *to_c_string(const nst::resource_descriptors &v) {
        static std::string result;
        result = "[";
        for (auto vi = v.begin(); vi != v.end(); ++vi) {
            if (vi != v.begin()) {
                result += ",";
            }
            result += std::to_c_string((*vi));

        }
        result += "]";
        return result.c_str();
    }
#endif
    struct fnv_1a {
        typedef unsigned char byte_type;
        // FNV-1a hash function for bytes
        // https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
        // https://tools.ietf.org/html/draft-eastlake-fnv-13#section-6

        size_t bytes(const byte_type *bytes, size_t count) const {
            const unsigned long long FNV64prime = 0x00000100000001B3ull;
            const unsigned long long FNV64basis = 0xCBF29CE484222325ull;
            std::size_t r = FNV64basis;
            for (size_t a = 0; a < count; ++a) {
                r ^= (size_t) bytes[a]; // folding of one byte at a time
                r *= FNV64prime;
            }
            return r;
        }

        /**
         *
         * @tparam _HType type that will be interpreted as raw bytes to hash
         * @param other the actual instance to be hashed
         * @return the
         */
        template<typename _HType>
        size_t hash(const _HType &other) const {
            const byte_type *other_bytes = reinterpret_cast<const byte_type *>(&other);
            return bytes(other_bytes, sizeof(other));
        }
    };

    template<>
    class hash<persist::storage::version_type> {
    private:

    public:
        size_t operator()(const persist::storage::version_type &version) const {
            fnv_1a hasher;
            size_t r = hasher.hash(version);
            return r;
        }
    };

};
#ifndef unlikely
#define unlikely(x) x
#endif
// safe logging methods based on std::variant
#define print_err(...)             do {  if (true) (console::println({console::cat({"[ERR][",nst::current_time(),"][",__FUNCTION__,"]"}), ##__VA_ARGS__ }, " ")); } while(0)
#define DBG_PRINT
#ifdef DBG_PRINT
#define print_dbg(...)             do {  if (unlikely(nst::storage_debugging)) (console::println({console::cat({"[DBG][",nst::current_time(),"][",__PRETTY_FUNCTION__,"]"}), ##__VA_ARGS__ }, " ")); } while(0)
#else
#define print_dbg(...)
#endif
#define print_wrn(...)             do {  if (unlikely(nst::storage_info)) (console::println({console::cat({"[WRN][",nst::current_time(),"][",__FUNCTION__,"]"}), ##__VA_ARGS__ }, " ")); } while(0)
#define print_inf(...)             do {  if (unlikely(nst::storage_info)) (console::println({console::cat({"[INF][",nst::current_time(),"][",__FUNCTION__,"]"}), ##__VA_ARGS__ }, " ")); } while(0)
// unsafe logging based on printf
#define dbg_print(x, ...)          do {  if (nst::storage_debugging) (printf("[DBG][%s][%s] " x "\n", nst::current_time().c_str(), __PRETTY_FUNCTION__, ##__VA_ARGS__)); } while(0)
#define wrn_print(x, ...)          do {  if (nst::storage_info) (printf("[WRN][%s][%s] " x "\n", nst::current_time().c_str(), __FUNCTION__, ##__VA_ARGS__)); } while(0)
#define err_print(x, ...)          do {  if (true) (printf("[ERR][%s][%s] " x "\n", nst::current_time().c_str(), __FUNCTION__, ##__VA_ARGS__)); } while(0)
#define inf_print(x, ...)          do {  if (nst::storage_info) (printf("[INF][%s][%s] " x "\n", nst::current_time().c_str(), __FUNCTION__, ##__VA_ARGS__)); } while(0)
#endif

///_STX_STORAGE_TYPES_H_

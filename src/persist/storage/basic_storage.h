#ifndef _STX_BSTORAGE_H_
#define _STX_BSTORAGE_H_
#ifdef _MSC_VER
#pragma warning(disable : 4503)
#endif

#include <persist/storage/types.h>
#include <persist/storage/leb128.h>
#include <lz4.h>
#include <stdio.h>
#include <string.h>
#include <persist/storage/pool.h>

/// memuse variables
extern persist::storage::u64 store_max_mem_use;
extern persist::storage::u64 store_current_mem_use;

extern persist::storage::u64 _reported_memory_size();

extern persist::storage::u64 calc_total_use();
//#include "storage/transactions/system_timers.h"


namespace persist {
    template<typename _Ht>
    struct btree_hash {
        size_t operator()(const _Ht &k) const {
            return (size_t) std::hash<_Ht>()(k); ///
        };
    };
    namespace storage {
        namespace allocation {

        }; /// allocations

        /// the general buffer type
        /// typedef std::vector<u8, sta::buffer_pool_alloc_tracker<u8>> buffer_type;
        typedef std::vector<u8> buffer_type;


        /// LZ4
        static inline void inplace_compress_lz4(buffer_type &buff, buffer_type &t) {
            typedef char *encode_type_ref;

            i32 origin = (i32) buff.size();
            /// TODO: cannot compress sizes lt 200 mb
            size_t dest_size = LZ4_compressBound((int) buff.size()) + sizeof(i32);
            if (t.size() < dest_size) t.resize(dest_size);
            i32 cp = buff.empty() ? 0 : LZ4_compress((const encode_type_ref) &buff[0],
                                                     (encode_type_ref) &t[sizeof(i32)], origin);
            *((i32 *) &t[0]) = origin;
            t.resize(cp + sizeof(i32));
            /// inplace_compress_zlibh(t);
            /// inplace_compress_fse(t);
            buff = t;

        }

        static inline void compress_lz4_fast(buffer_type &to, const buffer_type &from) {
            typedef char *encode_type_ref;
            static const i32 max_static = 8192;

            i32 origin = (i32) from.size();
            /// TODO: cannot compress sizes lt 200 mb
            i32 tcl = LZ4_compressBound((int) from.size()) + sizeof(i32);
            encode_type_ref to_ref;

            char s_buf[max_static];
            if (tcl < max_static) {
                to_ref = s_buf;
            } else {
                /// TODO: use a pool allocator directly
                to_ref = new char[tcl];
            }
            i32 cp = from.empty() ? 0 : LZ4_compress((const encode_type_ref) &from[0],
                                                     (encode_type_ref) &to_ref[sizeof(i32)], origin);
            *((i32 *) &to_ref[0]) = origin;
            if (to.empty()) {
                buffer_type temp((const u8 *) to_ref, ((const u8 *) to_ref) + (cp + sizeof(i32)));
                to.swap(temp);
            } else {
                to.resize(cp + sizeof(i32));
                memcpy(&to[0], to_ref, to.size());
            }
            if (tcl >= max_static) {
                delete to_ref;
            }
            /// inplace_compress_zlibh(t);
            /// inplace_compress_fse(t);
            //buff = t;

        }

        static inline void compress_lz4(buffer_type &to, const buffer_type &from) {
            typedef char *encode_type_ref;

            i32 origin = (i32) from.size();
            /// TODO: cannot compress sizes lt 200 mb
            to.resize(LZ4_compressBound((int) from.size()) + sizeof(i32));
            i32 cp = from.empty() ? 0 : LZ4_compress((const encode_type_ref) &from[0],
                                                     (encode_type_ref) &to[sizeof(i32)], origin);
            *((i32 *) &to[0]) = origin;
            to.resize(cp + sizeof(i32));
            /// inplace_compress_zlibh(t);
            /// inplace_compress_fse(t);
            //buff = t;

        }

        static inline void inplace_compress_lz4(buffer_type &buff) {

            buffer_type t;
            inplace_compress_lz4(buff, t);
        }

        static inline void decompress_lz4(buffer_type &decoded, const buffer_type &buff) { /// input <-> buff
            if (buff.empty()) {
                decoded.clear();
            } else {
                typedef char *encode_type_ref;
                /// buffer_type buff ;
                /// decompress_zlibh(buff, input);
                /// decompress_fse(buff, input);
                i32 d = *((i32 *) &buff[0]);
                decoded.reserve(d);
                decoded.resize(d);
                LZ4_decompress_fast((const encode_type_ref) &buff[sizeof(i32)], (encode_type_ref) &decoded[0], d);
            }

        }

        static inline size_t r_decompress_lz4(buffer_type &decoded, const buffer_type &buff) { /// input <-> buff
            if (buff.empty()) {
                decoded.clear();
            } else {
                typedef char *encode_type_ref;
                /// buffer_type buff ;
                /// decompress_zlibh(buff, input);
                /// decompress_fse(buff, input);
                i32 d = *((i32 *) &buff[0]);
                if ((i32) decoded.size() < d) {
                    i32 rs = d; //std::max<i32>(d,256000);
                    decoded.reserve(rs);
                    decoded.resize(rs);
                }
                LZ4_decompress_fast((const encode_type_ref) &buff[sizeof(i32)], (encode_type_ref) &decoded[0], d);
                return d;
            }
            return 0;
        }

        static inline void inplace_decompress_lz4(buffer_type &buff) {
            if (buff.empty()) return;
            buffer_type dt;
            decompress_lz4(dt, buff);
            buff = dt;
        }

        static inline void inplace_decompress_lz4(buffer_type &buff, buffer_type &dt) {
            if (buff.empty()) return;
            decompress_lz4(dt, buff);
            buff = dt;
        }

        static inline void test_compression() {

        }


        /// allocation type read only ,write or
        enum storage_action {
            read = 0,
            write,
            create
        };

        /// basic storage functions based on vectors of 1 byte unsigned
        class basic_storage {
        protected:
            typedef u8 value_type;

        public:
            /// reading functions for basic types

            buffer_type::iterator write(buffer_type::iterator out, u8 some) {
                return persist::storage::leb128::write_unsigned(out, some);
            }

            buffer_type::iterator write(buffer_type::iterator out, u16 some) {
                return persist::storage::leb128::write_unsigned(out, some);
            }

            buffer_type::iterator write(buffer_type::iterator out, u32 some) {
                return persist::storage::leb128::write_unsigned(out, some);
            }

            buffer_type::iterator write(buffer_type::iterator out, i8 some) {
                return persist::storage::leb128::write_signed(out, some);
            }

            /// reading functions for basic types
            u32 read_unsigned(buffer_type::iterator &inout) {
                return persist::storage::leb128::read_unsigned(inout);
            }

            i32 read_signed(buffer_type::iterator &inout) {
                return persist::storage::leb128::read_signed(inout);
            }

            size_t read(u8 *some, buffer_type::iterator in, buffer_type::iterator limit) {
                u8 *v = some;
                for (; in != limit; ++in) {
                    *v++ = *in;
                }
                return v - some;
            }
        };
        /// versions for all

        typedef std::pair<u64, version_type> _VersionRequest;

        typedef std::vector<_VersionRequest> _VersionRequests;
        /// some primitive encodings
        namespace primitive {

            /**
             * encodes at 8 bits/byte into orderable buffer
             * @tparam _PrimitiveInteger
             * @param todo
             * @param value
             * @return
             */
            template<typename _PrimitiveInteger, typename _IteratorType>
            size_t encode(_IteratorType todo, _IteratorType extent, _PrimitiveInteger value) {
                constexpr uint32_t vsize = sizeof(_PrimitiveInteger);
                _PrimitiveInteger bits = vsize << 3;
                auto o = todo;
                for (uint32_t i = 0; i < vsize; ++i) {
                    if (o == extent) {
                        print_err("buffer overflow");
                        break;
                    }
                    bits -= 8;
                    *o = ((uint8_t) (value >> bits));
                    ++o;
                }
                return o - todo;
            }

            template<typename _PrimitiveInteger, typename _IteratorType>
            size_t decode(_PrimitiveInteger &r, _IteratorType start, _IteratorType extent) {
                const uint32_t vsize = sizeof(_PrimitiveInteger);
                _IteratorType temp = start;
                r = _PrimitiveInteger();
                const _PrimitiveInteger mask = 0xffL;
                _PrimitiveInteger bits = vsize << 3;
                for (uint32_t i = 0; i < vsize; ++i) {
                    if (temp == extent) {
                        print_err("buffer overflow");
                        break;
                    }
                    bits -= 8;
                    r += ((*temp++ & mask) << bits);

                }
                return vsize;
            }

            template<typename _Primitive>
            static buffer_type::iterator store(buffer_type::iterator w, _Primitive p) {
                buffer_type::iterator writer = w;
                const u8 *s = (const u8 *) &p;
                const u8 *e = s + sizeof(_Primitive);
#if DEBUG
                auto w0 = std::copy(s, e, writer);
                size_t d0 = w0 - writer;
                assert(d0 == sizeof(_Primitive));
#else
                std::copy(s, e, writer);
#endif
                return writer + sizeof(_Primitive);
            }

            template<typename _Primitive>
            static buffer_type::const_iterator read(_Primitive &p, buffer_type::const_iterator r) {
                buffer_type::const_iterator reader = r;
                u8 *s = (u8 *) &p;
                std::copy(reader, reader + sizeof(_Primitive), s);
                reader += sizeof(_Primitive);
                return reader;
            }
        };// primitive
    }; //storage
};//persist
#endif

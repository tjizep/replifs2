//
// Created by Pretorius, Christiaan on 2020-07-02.
//

#ifndef REPLIFS_STRUCTURED_FILE_H
#define REPLIFS_STRUCTURED_FILE_H

#include <sys/stat.h>
#include <persist/btree_map>
#include <unordered_map>
#include <array>
#include <fstream>
#include <cstring>
//#include <filesystem>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace persist {
    namespace storage {
        namespace types{
            typedef nst::u64 version_id;
        }
        struct AllocationRecord {
            u64 size{0};
            u64 position{0};
            types::version_id version{0};
            
            AllocationRecord() {}

            AllocationRecord(u64 s, u64 p) : size(s), position(p), version(0) {}

            AllocationRecord(u64 s, u64 p, types::version_id v) : size(s), position(p), version(v) {}
            
            bool empty() const {
                return (position == 0);
            }

            bool operator<(const AllocationRecord &r) const {
                if (size < r.size)
                    return true;
                if (size == r.size)
                    return position < r.position;
                return false;
            }

            bool operator==(const AllocationRecord &r) const {
                return size == r.size && position == r.position;
            }

            bool operator!=(const AllocationRecord &r) const {
                return !(*this == r);
            }
            
            std::string to_string() const {
                return console::cat({size,position,version},",");
            }
        };

        namespace primitive {
            template<typename _IteratorType>
            size_t decode(AllocationRecord &r, const _IteratorType &in, const _IteratorType &extent) {
                size_t start = 0;
                start += decode<u64>(r.size, in + start, extent);
                start += decode<u64>(r.position, in + start, extent);
                start += decode<u64>(r.version, in + start, extent);
                return start;
            }

            template<typename _IteratorType>
            size_t encode(_IteratorType todo, _IteratorType extent, const AllocationRecord &value) {
                size_t start = 0;
                start += encode<u64>(todo + start, extent, value.size);
                start += encode<u64>(todo + start, extent, value.position);
                start += encode<u64>(todo + start, extent, value.version);
                return start;
            }
        }
    }
}
namespace persist {
    namespace storage {
        class structured_file {
        private:
            //mutable std::fstream data_file; // stream representing file
            int fd{-1};
            mutable off_t pos{0};
            std::string name;
        protected:
            mutable u64 error_count{0};
        public:
            u64 get_error_count() const {
                return error_count;
            }

            bool open_file(const std::string &file_name) {
                //data_file.open(file_name, std::ios::in | std::ios::out | std::ios::binary);
                fd = open(file_name.c_str(), O_RDWR | O_EXCL,
                          S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
                if (fd == -1) {
                    return check_error("open");
                }
                name = file_name;
                print_dbg("file",name,"fd",fd);
                return true;
            }

            u64 file_size() const {
                to_end();
                print_dbg("size",tell());
                return tell();
            }

            void close() {
                print_dbg("fd",fd);
                if (fd != -1) {
                    auto rval = ::close(fd);
                    print_dbg("rval",rval);
                    rval = 0;
                }
                name.clear();
            }


            bool check_error(const char *etype) const {
                if (errno != 0) {
                    ++error_count;
                    print_err("could not", etype, "file", name,"-", std::strerror(errno));
                    errno = 0;
                    return false;
                }

                return true;
            }

            void synch() {
                if (fd != -1) {

                }
            }

            bool create_file(const std::string &file_name) {
                // to create the file
                std::fstream f(file_name, std::ios::out | std::ios::binary);
                if (!f) {
                    return check_error("create");
                }
                f.flush();
                f.close();
                print_dbg("ok",file_name);
                return true;
            }

            bool exists(const std::string &name) const {
                errno = 0;
                int fd = open(name.c_str(), O_RDWR | O_EXCL,
                              S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

                if (fd == -1) {
                    print_dbg("not",name);
                    return false;
                }
                ::close(fd);
                print_dbg("ok",name);
                return true;
            }

            bool seek(u64 to, const char *where) const {
                if (error_count > 0) {
                    return false;
                }
                if(where){
                    print_dbg("seeking for ", where,"->", where, to);  
                }
                
                errno = 0;
                off_t at = ::lseek(fd, to, SEEK_SET);
                if (at > -1) pos = at;
                return check_error("seek");
            }

            u64 tell() const {
                if (error_count > 0) return 0;

                return pos;
            }

            template<typename _vT>
            bool read(_vT *buffer, size_t count) const {
                if (error_count > 0) return false;
                errno = 0;
                size_t start = 0;
                while (start != count) {
                    print_dbg("reading part", count - start,"bytes");
                    int r = ::read(fd, (void *) &buffer[0], count - start);
                    if (r == -1) {
                        return check_error("read");
                    }
                    start += r;
                    pos += r;
                }
                print_dbg("ok", count,"bytes");
                return true;
            }

            template<typename _vT>
            bool read_vec(_vT &buffer) {
                if (error_count > 0) return false;
                print_dbg("reading",buffer.size());
                return this->read(&buffer[0], buffer.size());
            }

            template<typename _vT>
            bool read_vec_at(_vT &buffer, u64 address, const char *where) {
                if (error_count > 0) return false;
                print_dbg("buffer size",buffer.size(),"address",address);
                if (!seek(address, where)) {
                    return check_error("read_at");
                }
                
                return read_vec(buffer);
            }

            template<typename _vT>
            bool read(_vT &primitive) const {
                if (error_count > 0) return false;
                print_dbg("primitive size",sizeof(_vT));
                return this->read<char>((char *) &primitive, sizeof(_vT));
            }

            template<typename _vT>
            bool set(const _vT &primitive) {
                if (error_count > 0) return false;
                std::array<u8, sizeof(_vT)> encoded;
                primitive::encode(encoded.begin(), encoded.end(), primitive);
                print_dbg("writing at", tell(), sizeof(_vT));
                return this->write((const char *) encoded.data(), sizeof(_vT));
            }

            template<typename _vT>
            _vT get() const {
                _vT r = _vT();
                std::array<u8, sizeof(_vT)> encoded;
                if (!read(encoded)) {
                    
                    return r;
                }
                print_dbg("get",sizeof(_vT),"->",encoded.size());
                primitive::decode(r, encoded.begin(), encoded.end());
                return r;
            }

            template<typename _vT>
            bool write(const _vT *buffer, const size_t count) {
                if (error_count > 0) return false;
                print_dbg("writing at", tell(), count);
                errno = 0;
                size_t start = 0;
                while (start != count) {
                    int r = ::write(fd, (const char *) &buffer[start], count - start);
                    if (r == -1) {
                        return check_error("write");
                    }
                    start += r;
                    pos += r;
                }
                return true;
            }

            template<typename _vT>
            bool write(const _vT &buffer) {
                return write(buffer.data(), buffer.size());
            }

            bool zero(size_t count) {
                if (error_count > 0) return false;
                static const u8 ZEROES[4096]{0};
                size_t remaining = count;
                print_dbg("count", count);
                while (remaining) {
                    size_t todo = std::min<size_t>(remaining, sizeof(ZEROES));
                    if (!write(&ZEROES[0], todo)) {
                        return false;
                    }
                    remaining -= todo;
                }
                return true;
            }

            bool to_end() const {
                if (error_count > 0) return false;
                errno = 0;
                
                off_t at = ::lseek(fd, 0, SEEK_END);
                print_dbg("at", at);
                if (at > -1) pos = at;
                return check_error("to end");
            }

            // TODO: implement 
            void resize(size_t /*newlen*/) {
                // truncate file
                //auto current = tell();
                //std::filesystem::resize_file(name, newlen);
                //seek(std::min<size_t>(newlen,current),"resize");
            }

            const std::string &get_name() const {
                return name;
            }
        };
    }
}
#endif //REPLIFS_STRUCTURED_FILE_H

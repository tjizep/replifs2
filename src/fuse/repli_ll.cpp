/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall hello_ll.c `pkg-config fuse --cflags --libs` -o hello_ll
*/

#define FUSE_USE_VERSION 26

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include <cstring>
#include <string.h>
#include <iostream>
#include <sstream>
#include <time.h>
#include <unistd.h>
#include "replifs.h"

//////////// WARNING : PROBABLY BAD HACK FROM STACK OVERFLOW ////////

#if defined(__MACH__) && !defined(CLOCK_REALTIME)
#include <sys/time.h>
#define CLOCK_REALTIME 0
// clock_gettime is not implemented on older versions of OS X (< 10.12).
// If implemented, CLOCK_REALTIME will have already been defined.
int clock_gettime(int /*clk_id*/, struct timespec* t) {
    struct timeval now;
    int rv = gettimeofday(&now, NULL);
    if (rv) return rv;
    t->tv_sec  = now.tv_sec;
    t->tv_nsec = now.tv_usec * 1000;
    return 0;
}
#endif
//////////// WARNING : PROBABLY BAD HACK FROM STACK OVERFLOW ////////

// a test block size to force splitting
#define REPLI_BLOCKSIZE 1024ull*32ull

std::shared_ptr<replifs::resources> repli;

static int repli_mknod(uint64_t context, const char *name, mode_t mode, dev_t dev, struct stat &st);

static int hello_stat(fuse_ino_t ino, struct stat *stbuf) {
    auto fio = repli->get_repli_fio(ino);
    if (fio == nullptr) {
        return -1;
    }
    memcpy(stbuf, &fio->st, sizeof(struct stat));
    return 0;
}

static void hello_ll_getattr(fuse_req_t req, fuse_ino_t ino,
                             struct fuse_file_info *fi) {
    struct stat stbuf{0};

    (void) fi;

    if (hello_stat(ino, &stbuf) == -1)
        fuse_reply_err(req, ENOENT);
    else
        fuse_reply_attr(req, &stbuf, 1.0);
}

static void hello_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
    struct fuse_entry_param e{0};
    if (!repli) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    uint64_t id = 0;
    if (!repli->get(parent, name, id)) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    if (hello_stat(id, &e.attr) == 0) {
        e.ino = e.attr.st_ino;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        fuse_reply_entry(req, &e);
        return;
    }
    fuse_reply_err(req, ENOENT);
}

struct dirbuf {
    char *p;
    size_t size;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
                       fuse_ino_t ino) {
    struct stat stbuf{0};
    size_t oldsize = b->size;
    b->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
    b->p = (char *) realloc(b->p, b->size);

    stbuf.st_ino = ino;
    fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf,
                      b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
                             off_t off, size_t maxsize) {
    if (off < bufsize)
        return fuse_reply_buf(req, buf + off,
                              min(bufsize - off, maxsize));
    else
        return fuse_reply_buf(req, NULL, 0);
}

static void hello_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                             off_t off, struct fuse_file_info *fi) {
    (void) fi;
    if (!repli) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    // TODO: check if ino is a directory
    if (fi->fh) {
        replifs::DirectoryState *d = (replifs::DirectoryState *) (void *) (fi->fh);
        //filler(buf, ".", NULL, 0);
        //filler(buf, "..", NULL, 0);
        thread_local std::string dir_data; // Eventually there will be no allocations
        dir_data.clear();
        size_t dsize = dir_data.size();
        size_t todo = size - off;
        size_t found = 0;
        for (; d->valid() && todo; d->next()) {
            size_t oldsize = dir_data.size();
            std::cout << "dir entry " << d->current_name() << std::endl;
            dsize += fuse_add_direntry(req, NULL, 0, d->current_name(), NULL, 0);
            dir_data.resize(dsize);
            auto fio = repli->get_repli_fio(d->current_id());
            if (fio) {
                fuse_add_direntry(req, &dir_data[oldsize], dsize - oldsize, d->current_name(), &fio->st, dsize);
            }
            --todo;
            ++found;
        }
        reply_buf_limited(req, dir_data.data(), dir_data.size(), off, size);
    } else {
        reply_buf_limited(req, NULL, 0, 0, 0);
    }
}

static void hello_ll_open(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi) {
    auto open = [&]() -> int {
        if (!repli)
            return ENOMEM;

        if (fi->fh) {
            return EBADF;
        }
        replifs::File *fio = repli->get_repli_fio(ino);
        if (!fio) {
            return ENOENT;// TODO: really invalid path
        }
        fi->fh = reinterpret_cast<uint64_t>(fio);
        uint64_t id = fio->st.st_ino;


        struct timespec current_time;
        if (clock_gettime(CLOCK_REALTIME, &current_time)) {
            return ENOMEM;
        }
        fio->st.st_atime = current_time.tv_sec;
        fio->st.st_atimensec = current_time.tv_nsec;
        return 0;
    };
    int r = open();
    if (r == 0)
        fuse_reply_open(req, fi);
    else
        fuse_reply_err(req, r);
}

static void hello_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size,
                          off_t _offset, struct fuse_file_info *fi) {
    auto read = [&]() -> int {
        if (!repli)
            return ENOMEM;
        if (!fi->fh) {
            return EBADF;
        }
        size_t len;
        replifs::File *fio = (replifs::File *) (void *) fi->fh;
        if (!fio) {
            return ENOMEM;
        }
        if (size + _offset > fio->st.st_size) {
            return EFAULT;
        }
        fio->rbuf.resize(size);
        char *pbuf = &fio->rbuf[0];
        uint64_t offset = _offset;
        uint64_t remaining = size;
        const uint64_t BS = REPLI_BLOCKSIZE;
        while (remaining) {// this loop will sometimes read block by block
            uint64_t ipos = offset % BS;
            uint64_t block = offset / BS;

            uint64_t todo = std::min<uint64_t>(BS - ipos, remaining);
            assert(ipos + todo <= BS);
            if (!repli->get_raw(fio->st.st_ino, block + replifs::Constants::DATA_OFFSET, ipos, pbuf, todo)) {
                return EIO;
            }
            assert(remaining + todo > remaining);
            remaining -= todo;
            pbuf += todo;
            offset += todo;
        }
        fuse_reply_buf(req, &fio->rbuf[0], size);
        return 0;
    };

    int r = read();
    if (r)
        fuse_reply_err(req, r);
}

static void repli_opendir(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi) {
    auto dir = repli->get_dir();
    auto fio = repli->get_repli_fio(ino);
    if (!fio) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    dir->open(ino);
    fi->fh = reinterpret_cast<uint64_t>(dir);
    fuse_reply_open(req, fi);


}

static void repli_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
    if (!repli) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    replifs::DirectoryState *d = (replifs::DirectoryState *) (void *) (fi->fh);
    repli->recycle_dir(d);
    fuse_reply_err(req, 0);
}

static void repli_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int valid, struct fuse_file_info *fi) {
    auto setattr = [&]() -> int {
        replifs::File *fio = repli->get_repli_fio(ino);
        if (!fio) {
            return ENOENT;// TODO: really invalid path
        }
        if (valid & FUSE_SET_ATTR_MODE) {
            fio->st.st_mode = attr->st_mode;
        }
        if (valid & FUSE_SET_ATTR_UID) {
            fio->st.st_uid = attr->st_uid;
        }
        if (valid & FUSE_SET_ATTR_GID) {
            fio->st.st_gid = attr->st_gid;
        }
        if (valid & FUSE_SET_ATTR_SIZE) {
            fio->st.st_size = attr->st_size;
        }
        if (valid & FUSE_SET_ATTR_ATIME) {
            fio->st.st_atimensec = attr->st_atimensec;
            fio->st.st_atime = attr->st_atime;
        }
        if (valid & FUSE_SET_ATTR_MTIME) {
            fio->st.st_mtimensec = attr->st_mtimensec;
            fio->st.st_mtime = attr->st_mtime;
        }
        auto r = repli->set(fio->st); /// update changes
        if (!r) {
            return EIO;
        }
        fuse_reply_attr(req, &fio->st, 1.0);
        return 0;
    };
    int r = setattr();
    if (r)
        fuse_reply_err(req, r);
}

static int repli_mknod(uint64_t context, const char *name, mode_t mode, dev_t dev, struct stat &st) {
    if (!repli)
        return ENOMEM;
    // path can be as long as it likes - longer the worse but thats up to the user
    struct timespec current_time;

    if (clock_gettime(CLOCK_REALTIME, &current_time)) {
        return ENOMEM;
    }

    uint64_t id = repli->create();
    st.st_ino = id;
    st.st_mode = mode; // & PERMMASK;
    st.st_dev = dev;
    st.st_birthtime = current_time.tv_sec;
    st.st_birthtimensec = current_time.tv_nsec;
    st.st_atime = current_time.tv_sec;
    st.st_atimensec = current_time.tv_nsec;
    st.st_ctime = current_time.tv_sec;
    st.st_ctimensec = current_time.tv_nsec;
    st.st_mtime = current_time.tv_sec;
    st.st_mtimensec = current_time.tv_nsec;

    st.st_blksize = 4 * 1024; //REPLI_BLOCKSIZE;
    st.st_nlink = 1;
    if (mode & S_IFREG) {
        st.st_nlink = 1;
    } else if (mode & S_IFDIR) {
        st.st_nlink = 2;
    }
    auto cr = repli->create(context, name, id, nullptr, 0);
    if (cr.first) {
        repli->set(st);
        replifs::File *fio = repli->get_repli_fio(id);
        if (!fio) {
            return ENOENT;// TODO: really invalid path
        }
        return 0;
    }

    return ENOMEM;
}

static int repli_mknod(uint64_t context, const char *name, mode_t mode, dev_t dev) {
    struct stat st{0};
    return repli_mknod(context, name, mode, dev, st);
}

static void repli_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
    struct stat st{0};
    struct fuse_entry_param e{0};

    int r = repli_mknod(parent, name, mode, rdev, st);
    if (r == 0) {
        e.attr = st;// more typesafe in c++ if it fails then struct stat != struct stat
        e.ino = e.attr.st_ino;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        fuse_reply_entry(req, &e);
    } else {
        fuse_reply_err(req, r);
    }
}

static void repli_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
    repli_mknod(req, parent, name, mode, 0);
}

static void repli_unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
    if (repli == nullptr) {
        fuse_reply_err(req, ENOMEM);
        return;
    }
    if (!repli->remove(parent, name)) {
        fuse_reply_err(req, ENOENT);
    } else {
        fuse_reply_err(req, 0);
    }
}

static void repli_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
    repli_unlink(req, parent, name);
}

static void
repli_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname) {
    auto rename = [&]() -> int {
        uint64_t id = 0;
        if (!repli->get(parent, name, id)) {
            return ENOENT;
        }
        if (!repli->remove(parent, name)) {
            return EIO;
        }
        if (!repli->create(newparent, newname, id, nullptr, 0).first) {
            return EIO;
        }
        return 0;
    };
    int r = rename();
    if (r != 0) {
        fuse_reply_err(req, r);
    } else {
        fuse_reply_err(req, 0);
    }
}

static void repli_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname) {
    auto link = [&]() -> int {

        if (!repli->create(newparent, newname, ino, nullptr, 0).first) {
            return EIO;
        }
        return 0;
    };
    int r = link();
    if (r != 0) {
        fuse_reply_err(req, r);
    } else {
        fuse_reply_err(req, 0);
    }
}

static int storage_write(fuse_ino_t ino, const char *buf, size_t size, off_t _offset) {
    if (!repli)
        return ENOMEM;
    replifs::File *fio = nullptr;
    fio = repli->get_repli_fio(ino);

    if (!fio) {
        return ENOENT;
    }
    if (_offset > fio->st.st_size) {
        return EFAULT;
    }
    const char *ibuf = buf;
    uint64_t offset = _offset;
    uint64_t remaining = size;
    const uint64_t BS = REPLI_BLOCKSIZE;
    struct timespec current_time;
    if (clock_gettime(CLOCK_REALTIME, &current_time)) {
        return EFAULT;
    }
    fio->st.st_mtime = current_time.tv_sec;
    fio->st.st_mtimensec = current_time.tv_nsec;

    while (remaining) {// this loop will sometimes read then write block by block

        uint64_t ipos = offset % BS;
        uint64_t block = offset / BS;

        uint64_t todo = std::min<uint64_t>(BS - ipos, remaining);
        assert(ipos + todo <= BS);

        if (!repli->set_anon_raw(fio->st.st_ino, block + replifs::Constants::DATA_OFFSET, ibuf, todo, ipos)) {
            return EIO;
        }
        assert(remaining + todo > remaining);
        remaining -= todo;
        ibuf += todo;
        offset += todo;
    }

    fio->st.st_size = std::max<size_t>(fio->st.st_size, _offset + size);
    //auto r = repli->set(fio); /// update changes
    //if(!r){
    //    return EIO;
    //}
    return 0;
};

static void repli_write
        (fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t _offset, struct fuse_file_info *fi
        ) {
    replifs::File *fio = nullptr;
    if (!fi->fh) {
        fio = repli->get_repli_fio(ino);
    } else {
        fio = (replifs::File *) (void *) fi->fh;
    }

    if (!fio) {
        fuse_reply_err(req, EBADF);
        return;
    }


    int r = storage_write(ino, buf, size, _offset);

    if (r == 0) {
        fio->st.st_size = std::max<size_t>(fio->st.st_size, _offset + size);
        fuse_reply_write(req, size);
    } else {
        fuse_reply_err(req, r);
    }
}

static void repli_flush(fuse_req_t req, fuse_ino_t ino,
                        struct fuse_file_info *fi) {
    replifs::File *fio = nullptr;
    if (!fi->fh) {
        fio = repli->get_repli_fio(ino);
    } else {
        fio = (replifs::File *) (void *) fi->fh;
    }

    if (!fio) {
        fuse_reply_err(req, EBADF);
        return;
    }

    auto r = repli->set(fio); /// update changes
    if (!r) {
        fuse_reply_err(req, EIO);
        return;
    }
    fuse_reply_err(req, 0);
}

static void repli_release(fuse_req_t req, fuse_ino_t ino,
                          struct fuse_file_info *fi) {
    replifs::File *fio = nullptr;
    if (!fi->fh) {
        fio = repli->get_repli_fio(ino);
    } else {
        fio = (replifs::File *) (void *) fi->fh;
    }

    if (!fio) {
        fuse_reply_err(req, EBADF);
        return;
    }

    auto s = repli->set(fio); /// update changes
    if (!s) {
        fuse_reply_err(req, EIO);
        return;
    }
    int r = 0;

    fuse_reply_err(req, r);
}

static void repli_statfs(fuse_req_t req, fuse_ino_t ino) {

    char cdir[4096];

    struct statvfs outer{0};
    statvfs(getcwd(cdir, sizeof(cdir)), &outer);
    struct statvfs vfs{0};

    const uint32_t bsize = 512;
    outer.f_namemax = 4096;
    vfs.f_bavail = outer.f_bavail;
    vfs.f_bfree = outer.f_bfree;
    vfs.f_blocks = outer.f_blocks;
    vfs.f_bsize = outer.f_bsize;
    fuse_reply_statfs(req, &vfs);
}

static void repli_access(fuse_req_t req, fuse_ino_t ino, int mask) {
// TODO: just allow all
    fuse_reply_err(req, 0);
}

static void repli_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                         mode_t mode, struct fuse_file_info *fi) {
    if (repli == nullptr) return;
    struct stat st{0};
    int r = repli_mknod(parent, name, mode, 0, st);
    if (r == 0) {
        auto fio = repli->get_repli_fio(st.st_ino);
        if (fio) {
            fi->fh = (uint64_t) (void *) fio;
            fuse_entry_param e{0};
            e.attr = st;// more typesafe in c++ if it fails then struct stat != struct stat
            e.ino = e.attr.st_ino;
            e.attr_timeout = 1.0;
            e.entry_timeout = 1.0;
            fuse_reply_create(req, &e, fi);
        } else {
            fuse_reply_err(req, ENOENT);
        }
    } else {
        fuse_reply_err(req, r);
    }
}

void repli_init(void *userdata, struct fuse_conn_info *conn) {
    using namespace replifs;
    repli = std::make_shared<replifs::resources>();
}

static struct fuse_lowlevel_ops hello_ll_oper = {
        .lookup        = hello_ll_lookup,
        .getattr    = hello_ll_getattr,
        .readdir    = hello_ll_readdir,
        .open        = hello_ll_open,
        .read        = hello_ll_read,
        .opendir    = repli_opendir,
        .releasedir = repli_releasedir,
        .setattr    = repli_setattr,
        .mknod      = repli_mknod,
        .mkdir      = repli_mkdir,
        .unlink     = repli_unlink,
        .rmdir      = repli_rmdir,
        .rename     = repli_rename,
        .link       = repli_link,
        .write      = repli_write,
        .flush      = repli_flush,
        .release    = repli_release,
        .statfs     = repli_statfs,
        .access     = repli_access,
        .create     = repli_create,
        .init       = repli_init,
};

int main(int argc, char *argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_chan *ch;
    char *mountpoint;
    int err = -1;

    if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
        (ch = fuse_mount(mountpoint, &args)) != NULL) {
        struct fuse_session *se;

        se = fuse_lowlevel_new(&args, &hello_ll_oper,
                               sizeof(hello_ll_oper), NULL);
        if (se != NULL) {
            if (fuse_set_signal_handlers(se) != -1) {
                fuse_session_add_chan(se, ch);
                err = fuse_session_loop(se);
                fuse_remove_signal_handlers(se);
                fuse_session_remove_chan(ch);
            }
            fuse_session_destroy(se);
        }
        fuse_unmount(mountpoint, ch);
    }
    fuse_opt_free_args(&args);

    return err ? 1 : 0;
}

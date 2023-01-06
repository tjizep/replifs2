//#include "config.h"

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
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
#define REPLI_BLOCKSIZE 1024*2

std::shared_ptr<replifs::resources> repli;

static int repli_mknod(const char *path, mode_t mode, dev_t dev, struct stat &st);

static int repli_mknod(const char *path, mode_t mode, dev_t dev);

static int hello_getattr(const char *path, struct stat *stbuf) {
    if (!repli) return -ENOMEM;
    if (!path) return 0;
    memset(stbuf, 0, sizeof(struct stat));
    replifs::File *fio = repli->get_repli_fio(path);
    if (fio) {
        memcpy(stbuf, &fio->st, sizeof(struct stat));
        return 0;
    }


    return -ENOENT;
}

static int repli_setattr(const char *path, struct stat *stbuf) {
    if (!repli)
        return -ENOMEM;

    replifs::File *fio = repli->get_repli_fio(path);
    if (fio) {
        auto r = repli->set(*stbuf); /// update changes
        if (!r) {
            return -EIO;
        }
        memcpy(&fio->st, stbuf, sizeof(struct stat));
        return 0;
    }
    return -ENOENT;

}

static int repli_opendir(const char *path, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;
    auto dir = repli->get_dir();
    if (dir->open(path)) {
        fi->fh = reinterpret_cast<uint64_t>(dir);
        return 0;
    }

    // error
    return -ENOENT;
}

static int repli_releasedir(const char *path, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;
    replifs::DirectoryState *d = (replifs::DirectoryState *) (void *) (fi->fh);
    repli->recycle_dir(d);

    return 0;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
    (void) offset;
    if (!repli)
        return -ENOMEM;
    if (fi->fh) {
        replifs::DirectoryState *d = (replifs::DirectoryState *) (void *) (fi->fh);
        //filler(buf, ".", NULL, 0);
        //filler(buf, "..", NULL, 0);
        for (; d->valid(); d->next()) {
            filler(buf, d->current_name(), NULL, 0);
        }
    }
    return 0;
}


static int hello_open(const char *path, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;

    if (fi->fh) {
        return -EBADF;
    }
    replifs::File *fio = repli->get_repli_fio(path);
    if (!fio) {
        return -ENOENT;// TODO: really invalid path
    }
    fi->fh = reinterpret_cast<uint64_t>(fio);
    uint64_t id = fio->st.st_ino;


    struct timespec current_time;
    if (clock_gettime(CLOCK_REALTIME, &current_time)) {
        return -ENOMEM;
    }
    fio->st.st_atime = current_time.tv_sec;
    fio->st.st_atimensec = current_time.tv_nsec;

    return 0;
}

int repli_release(const char *path, struct fuse_file_info *fi) {
    if (fi->fh) {
        replifs::File *fio = (replifs::File *) (void *) fi->fh;
        auto r = repli->set(fio); /// update changes
        if (!r) {
            return -EIO;
        }
    }
    return 0;
}

static int repli_read(const char *path, char *buf, size_t size, off_t _offset, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;
    if (!fi->fh) {
        int r = hello_open(path, fi);
        if (r != 0)
            return r;
    }
    size_t len;
    replifs::File *fio = (replifs::File *) (void *) fi->fh;
    if (!fio) {
        return -ENOMEM;
    }

    if (size + _offset > fio->st.st_size) {
        return -EFAULT;
    }
    char *pbuf = buf;
    uint64_t offset = _offset;
    uint64_t remaining = size;
    while (remaining) {// this loop will sometimes read block by block
        uint64_t ipos = offset % REPLI_BLOCKSIZE;
        uint64_t block = offset / REPLI_BLOCKSIZE;

        uint64_t todo = std::min<uint64_t>(REPLI_BLOCKSIZE - ipos, remaining);
        assert(ipos + todo <= REPLI_BLOCKSIZE);
        if (!repli->get(fio->st.st_ino, block + replifs::Constants::DATA_OFFSET, ipos, pbuf, todo)) {
            return -EIO;
        }
        assert(remaining + todo > remaining);
        remaining -= todo;
        pbuf += todo;
        offset += todo;
    }

    return size;
}

static int repli_write(const char *path, const char *data, size_t size, off_t _offset, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;
    if (!fi->fh) {
        int r = hello_open(path, fi);
        if (r != 0)
            return r;
    }
    replifs::File *fio = (replifs::File *) (void *) fi->fh;
    if (!fio) {
        return -ENOMEM;
    }
    if (_offset > fio->st.st_size) {
        return -EFAULT;
    }
    const char *ibuf = data;
    uint64_t offset = _offset;
    uint64_t remaining = size;
    struct timespec current_time;
    if (clock_gettime(CLOCK_REALTIME, &current_time)) {
        return -ENOMEM;
    }
    fio->st.st_mtime = current_time.tv_sec;
    fio->st.st_mtimensec = current_time.tv_nsec;

    while (remaining) {// this loop will sometimes read then write block by block

        uint64_t ipos = offset % REPLI_BLOCKSIZE;
        uint64_t block = offset / REPLI_BLOCKSIZE;

        uint64_t todo = std::min<uint64_t>(REPLI_BLOCKSIZE - ipos, remaining);
        assert(ipos + todo <= REPLI_BLOCKSIZE);
        if (todo != REPLI_BLOCKSIZE && _offset < fio->st.st_size) {

            repli->get(fio->st.st_ino, block + replifs::Constants::DATA_OFFSET, fio->data);
        } else {
            //fio->data.resize(REPLI_BLOCKSIZE);
        }

        assert(fio->data.size() <= REPLI_BLOCKSIZE);
        fio->data.clear();
        fio->data.resize(std::max<size_t>(fio->data.size(), ipos + todo));
        memcpy(&fio->data[ipos], ibuf, todo);
        //if(!repli->set_anon(fio->st.st_ino, block+replifs::Constants::DATA_OFFSET, fio->data)){
        //    return -EIO;
        //}
        assert(remaining + todo > remaining);
        remaining -= todo;
        ibuf += todo;
        offset += todo;
    }

    fio->st.st_size = std::max<size_t>(fio->st.st_size, _offset + size);
    //auto r = repli->set(fio); /// update changes
    //if(!r){
    //    return -EIO;
    //}
    return size;
}

static int repli_fsyncdir(const char *, int, struct fuse_file_info *) {
    return 0;
}

static void *repli_init(struct fuse_conn_info *conn) {
    using namespace replifs;
    repli = std::make_shared<replifs::resources>();
    return repli.get();
}

static void repli_destroy(void *) {
    repli = nullptr;
}

static int repli_access(const char *path, int flags) {
    return 0;
}

static int repli_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;

    auto r = repli_mknod(path, mode, 0);
    if (r == 0) {
        replifs::File *fio = repli->get_repli_fio(path);
        if (!fio) {
            return -ENOENT;// TODO: really invalid path
        }
        fi->fh = reinterpret_cast<uint64_t>(fio);
    }

    return r;
}

static int repli_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) {
    if (!repli)
        return -ENOMEM;
    replifs::File *fio = repli->get_repli_fio(path);
    if (!fio) {
        return -ENOENT;// TODO: really invalid path
    }

    fio->st.st_size = offset;
    repli->set(path, (char *) &fio->st, sizeof(fio->st));

    return 0;
}

static int repli_truncate(const char *path, off_t offset) {
    if (!repli)
        return -ENOMEM;
    replifs::File *fio = repli->get_repli_fio(path);
    if (!fio) {
        return -ENOENT;// TODO: really invalid path
    }
    fio->st.st_size = offset;
    repli->set(fio);

    return 0;
}

static int repli_readlink(const char *path, char *name, size_t len) {
    if (!repli)
        return -ENOMEM;
    replifs::File *fio = repli->get_repli_fio(path);
    if (!fio) {
        return -ENOENT;// TODO: really invalid path
    }


    return 0;
}

static void new_stat(struct stat *s) {

}

static int repli_mknod(const char *path, mode_t mode, dev_t dev, struct stat &st) {
    if (!repli)
        return -ENOMEM;
    // path can be as long as it likes - longer the worse but thats up to the user
    struct timespec current_time;

    if (clock_gettime(CLOCK_REALTIME, &current_time)) {
        return -ENOMEM;
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

    st.st_blksize = 2 * 1024; //REPLI_BLOCKSIZE;
    st.st_nlink = 1;
    if (mode & S_IFREG) {
        st.st_nlink = 1;
    } else if (mode & S_IFDIR) {
        st.st_nlink = 2;
    }
    auto cr = repli->create(path, id, nullptr, 0);
    if (cr.first) {
        repli->set(st);
        replifs::File *fio = repli->get_repli_fio(path);
        if (!fio) {
            return -ENOENT;// TODO: really invalid path
        }
        return 0;
    }

    return -ENOMEM;
}

static int repli_mknod(const char *path, mode_t mode, dev_t dev) {
    struct stat st{0};
    return repli_mknod(path, mode, dev, st);
}

static int repli_mkdir(const char *path, mode_t mode) {
    return repli_mknod(path, mode | S_IFDIR, 0);
}

static int repli_unlink(const char *path) {
    if (!repli)
        return -ENOMEM;

    if (repli->remove(path)) {
        return 0;
    }
    return -ENOENT;
}

static int repli_rmdir(const char *path) {
    if (!repli)
        return -ENOMEM;

    if (repli->remove(path)) {
        return 0;
    }
    return -ENOENT;
}

static int repli_rename(const char *path, const char *npath) {
    struct stat st{0};
    uint64_t id = 0;
    //if(repli->exists(npath)){
    //    return -EIO;
    //}
    repli->get(path, id, (char *) &st, sizeof(struct stat));
    if (!id) {
        return -ENOENT;
    }
    if (!repli->remove(path)) {
        return -EBADF;
    }
    auto cr = repli->create(npath, id, (const char *) &st, sizeof(st));
    if (cr.first) {
        return 0;
    }

    return -ENOMEM;
}

static int repli_link(const char *path_from, const char *path_to) {
    struct stat st{0};
    uint64_t id = 0;
    repli->get(path_from, id, (char *) &st, sizeof(struct stat));
    if (!id) {
        return -ENOENT;
    }
    auto cr = repli->create(path_to, id, (const char *) &st, sizeof(st));
    if (cr.first)
        return 0;
    return -ENOMEM;
}

/** Change the permission bits of a file */
static int repli_chmod(const char *path, mode_t mode) {
    if (!repli) return -ENOMEM;

    replifs::File *fio = repli->get_repli_fio(path);
    if (!fio) {
        return -ENOENT;// TODO: really invalid path
    }

    fio->st.st_mode = mode;
    if (!repli->set(path, (char *) &fio->st, sizeof(fio->st))) {
        return -ENOMEM;
    }
    return 0;
}

/** Change the owner and group of a file */
static int repli_chown(const char *path, uid_t uid, gid_t gid) {
    if (!repli)
        return -ENOMEM;
    replifs::File *fio = repli->get_repli_fio(path);
    if (!fio) {
        return -ENOENT;// TODO: really invalid path
    }

    fio->st.st_uid = uid;
    fio->st.st_gid = gid;

    if (!repli->set(fio)) {
        return -ENOMEM;
    }
    return 0;

}

static int repli_statfs(const char *name, struct statvfs *vfs) {
    if (vfs == nullptr) return -ENOMEM;

    char cdir[4096];

    struct statvfs outer{0};
    statvfs(getcwd(cdir, sizeof(cdir)), &outer);

    memset(vfs, 0, sizeof(struct statvfs));
    const uint32_t bsize = 512;
    vfs->f_namemax = 4096;
    vfs->f_bavail = outer.f_bavail;
    vfs->f_bfree = outer.f_bfree;
    vfs->f_blocks = outer.f_blocks;
    vfs->f_bsize = outer.f_bsize;
    return 0;
}

static struct fuse_operations hello_oper = {
        .getattr    = hello_getattr,
        .readdir    = hello_readdir,
        .readlink   = repli_readlink,
        .mknod      = repli_mknod,
        .mkdir      = repli_mkdir,
        .unlink     = repli_unlink,
        .rmdir      = repli_rmdir,
        .rename     = repli_rename,
        .link       = repli_link,
        .chmod      = repli_chmod,
        .chown      = repli_chown,
        .truncate   = repli_truncate,
        .ftruncate  = repli_ftruncate,
        .statfs     = repli_statfs,
        .open        = hello_open,
        .release    = repli_release,
        .read        = repli_read,
        .write      = repli_write,
        .opendir    = repli_opendir,
        .releasedir = repli_releasedir,
        .create     = repli_create,
        .fsyncdir   = repli_fsyncdir,
        .init       = repli_init,
        .destroy    = repli_destroy,
        .access     = repli_access,

};

int main(int argc, char *argv[]) {

    return fuse_main(argc, argv, &hello_oper, NULL);
}
//
// Created by Pretorius, Christiaan on 2022-08-13.
//

#ifndef REPLIFS_BT_TX_CTX_H
#define REPLIFS_BT_TX_CTX_H
#include "storage/transaction.h"
namespace tests {
    class bt_tx_ctx : public nst::transaction {
    public:
        bt_tx_ctx(nst::file_storage_alloc *fa)
                : nst::transaction(fa) {

        }

        ~bt_tx_ctx() {
        }

        /// bt paging functions
        template<class PageType>
        PageType* get_page(nst::stream_address /*addrs*/){
            return nullptr;
        }
        template<class PageType, class AllocatorType>
        void set_page(nst::stream_address /*addrs*/, PageType * /*page*/){

        }
    };

}
#endif //REPLIFS_BT_TX_CTX_H
